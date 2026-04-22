import scala.annotation.tailrec
import scala.io.Source
import scala.collection.parallel.CollectionConverters.*

import scala.collection.parallel.ForkJoinTaskSupport
import java.util.concurrent.ForkJoinPool
import scala.sys.exit

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
    oracleWriter.truncateTable()

    case class ProcessState(
                             processedCount: Int,
                             totalOriginal: Double,
                             totalDiscount: Double,
                             totalFinal: Double,
                             totalInserted: Int
                           )

    val batchSize = 50000
    val coreCount = Runtime.getRuntime.availableProcessors()
    logger.info(s"Using ForkJoin parallelism: $coreCount")

    // One dedicated pool — sized to core count, not shared with JVM internals
    val fjPool       = new ForkJoinPool(coreCount)
    val taskSupport  = new ForkJoinTaskSupport(fjPool)

    @tailrec
    def processBatches(lines: Iterator[String], state: ProcessState): ProcessState = {
      if (!lines.hasNext) return state

      val batchLines = lines.take(batchSize).toVector
      if (batchLines.isEmpty) return state

      // Attach the dedicated pool — this is the key configuration step
      val parBatch = batchLines.par
      parBatch.tasksupport = taskSupport

      // Pure CPU work runs in parallel across all cores
      val processed = parBatch
        .flatMap(OrderParser.fromCsvLine(_).toOption)
        .map(orderProcessor.calculateFinalPrice)
        .toList

      val summary = processed.foldLeft((0.0, 0.0, 0.0)) {
        case ((orig, disc, fin), order) =>
          (orig + order.order.unitPrice * order.order.quantity,
            disc + order.discountAmount,
            fin + order.finalPrice)
      }

      // DB write stays on the main thread — single connection, must be sequential
      val inserted = oracleWriter.insertOrders(processed, tableName)
      logger.info(s"Batch done: ${processed.length} processed, $inserted inserted")

      processBatches(lines, ProcessState(
        processedCount = state.processedCount + processed.length,
        totalOriginal  = state.totalOriginal  + summary._1,
        totalDiscount  = state.totalDiscount  + summary._2,
        totalFinal     = state.totalFinal     + summary._3,
        totalInserted  = state.totalInserted  + inserted
      ))
    }

    logger.info("Reading and processing CSV file in batches...")
    logger.info(s"Batch size: $batchSize records")

    val source = Source.fromFile("src/main/resources/TRX1000.csv")(scala.io.Codec.UTF8)

    try {
      val lines = source.getLines()
      if (lines.hasNext) lines.next() // skip header

      val finalState = processBatches(lines, ProcessState(0, 0.0, 0.0, 0.0, 0))

      logger.info("=== Summary ===")
      logger.info(f"Total Original Amount: $$${finalState.totalOriginal}%.2f")
      logger.info(f"Total Discount Amount: $$${finalState.totalDiscount}%.2f")
      logger.info(f"Total Final Amount:    $$${finalState.totalFinal}%.2f")
      logger.info(s"Total Orders Processed: ${finalState.processedCount}")
      logger.info(s"Total Rows Inserted:    ${finalState.totalInserted}")

      val totalDuration = System.currentTimeMillis() - startTime
      val avgThroughput = finalState.processedCount * 1000 / (totalDuration + 1)
      logger.info(s"TOTAL TIME:     ${totalDuration}ms (${totalDuration / 1000.0}s)")
      logger.info(s"AVG THROUGHPUT: $avgThroughput orders/sec")
      logger.info("=== Order Discount System Completed Successfully ===")

    } finally {
      source.close()
      fjPool.shutdown()
      exit(0)
    }

  } catch {
    case e: Exception =>
      logger.error(s"Application failed: ${e.getMessage}")
      e.printStackTrace()
  }
}