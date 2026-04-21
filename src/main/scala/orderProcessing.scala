import scala.annotation.tailrec
import scala.io.Source
import scala.collection.parallel.CollectionConverters.*

object orderProcessing extends App {

  val logger = new Logger()
  val startTime = System.currentTimeMillis()
  logger.info("=== Order Discount System Started ===")

  try {
    logger.info("Building discount rules...")
    val rules = RulesBuilder().getRules
    val orderProcessor = OrderProcessor(rules)

    logger.info("Connecting to Oracle database...")
    import DatabaseConfig._
    val oracleWriter = new OracleWriter(url, user, password)
    oracleWriter.createTableIfNotExists(tableName)

    // State as immutable case class
    case class ProcessState(
                             processedCount: Int,
                             totalOriginal: Double,
                             totalDiscount: Double,
                             totalFinal: Double,
                             totalInserted: Int
                           )

    @tailrec
    def processBatches(
                        lines: Iterator[String],
                        batchSize: Int,
                        state: ProcessState
                      ): ProcessState = {
      if (!lines.hasNext) state
      else {
        val batchLines = lines.take(batchSize).toList
        if (batchLines.isEmpty) state
        else {
          // Parse batch in parallel
          val orders = batchLines.par.flatMap { line =>
            OrderParser().fromCsvLine(line).toOption
          }.toList

          // Process orders in parallel
          val processedOrders = orders.par.map(orderProcessor.calculateFinalPrice).toList

          // Calculate batch summary using parallel aggregation
          val (batchOriginal, batchDiscount, batchFinal) = processedOrders.par.aggregate((0.0, 0.0, 0.0))(
            { case ((orig, disc, fin), order) =>
              (orig + order.order.unitPrice * order.order.quantity, disc + order.discountAmount, fin + order.finalPrice)
            },
            { case ((o1, d1, f1), (o2, d2, f2)) =>
              (o1 + o2, d1 + d2, f1 + f2)
            }
          )

          // Insert batch
          val insertedCount = if (processedOrders.nonEmpty) {
            oracleWriter.insertNewOrdersOnly(processedOrders, tableName)
          } else 0

          logger.info(s"Processed batch: ${processedOrders.length} orders, Inserted: $insertedCount")

          // Recursively process next batch
          processBatches(lines, batchSize, ProcessState(
            processedCount = state.processedCount + processedOrders.length,
            totalOriginal = state.totalOriginal + batchOriginal,
            totalDiscount = state.totalDiscount + batchDiscount,
            totalFinal = state.totalFinal + batchFinal,
            totalInserted = state.totalInserted + insertedCount
          ))
        }
      }
    }

    logger.info("Reading and processing CSV file in batches...")
    val source = Source.fromFile("D:\\iti\\24.FP with Scala\\order-discount-system\\src\\main\\resources\\TRX10M.csv")

    try {
      val lines = source.getLines()

      // Skip header
      val dataLines = if (lines.hasNext) {
        lines.next() // consume header
        lines
      } else {
        Iterator.empty
      }

      val batchSize = 10000 // Adjust based on available memory
      val finalState = processBatches(dataLines, batchSize, ProcessState(0, 0.0, 0.0, 0.0, 0))

      // Final summary
      logger.info("=== Summary ===")
      logger.info(f"Total Original Amount: $$${finalState.totalOriginal}%.2f")
      logger.info(f"Total Discount Amount: $$${finalState.totalDiscount}%.2f")
      logger.info(f"Total Final Amount: $$${finalState.totalFinal}%.2f")
      logger.info(s"Total Orders Processed: ${finalState.processedCount}")
      logger.info(s"Total Rows Inserted: ${finalState.totalInserted}")

      val totalDuration = System.currentTimeMillis() - startTime
      logger.info(s"TOTAL TIME:       ${totalDuration}ms (${totalDuration / 1000.0}s)")
      logger.info("=== Order Discount System Completed Successfully ===")

    } finally {
      source.close()
    }

  } catch {
    case e: Exception =>
      logger.error(s"Application failed: ${e.getMessage}")
      e.printStackTrace()
  }
}