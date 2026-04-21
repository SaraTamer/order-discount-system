import scala.annotation.tailrec
import scala.io.Source
import scala.collection.parallel.CollectionConverters.*
import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*
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

    // State as immutable case class
    case class ProcessState(
                             processedCount: Int,
                             totalOriginal: Double,
                             totalDiscount: Double,
                             totalFinal: Double,
                             totalInserted: Int
                           )

    // Increase batch size for better throughput
    val batchSize = 50000 // 50K records per batch (adjust based on memory)
    val parallelismLevel = Runtime.getRuntime.availableProcessors()
    logger.info(s"Using parallelism level: $parallelismLevel")

    // Create a thread pool for parallel batch processing
    val batchParallelism = 4 // Process 4 batches concurrently
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(
      Executors.newFixedThreadPool(batchParallelism)
    )

    def processBatchesParallel(
                                lines: Iterator[String],
                                batchSize: Int,
                                state: ProcessState
                              ): ProcessState = {
      @tailrec
      def processBatchGroup(
                             remainingLines: Iterator[String],
                             currentState: ProcessState,
                             batchFutures: List[Future[(List[ProcessedOrder], (Double, Double, Double))]]
                           ): ProcessState = {
        if (batchFutures.isEmpty && !remainingLines.hasNext) {
          currentState
        } else if (batchFutures.size < batchParallelism && remainingLines.hasNext) {
          // Start new batch
          val batchLines = remainingLines.take(batchSize).toVector
          if (batchLines.isEmpty) {
            processBatchGroup(remainingLines, currentState, batchFutures)
          } else {
            val future = Future {
              val orders = batchLines.par.flatMap(OrderParser.fromCsvLine(_).toOption).toList
              val processed = orders.par.map(orderProcessor.calculateFinalPrice).toList
              val summary = processed.foldLeft((0.0, 0.0, 0.0)) {
                case ((orig, disc, fin), order) =>
                  (orig + order.order.unitPrice * order.order.quantity,
                    disc + order.discountAmount,
                    fin + order.finalPrice)
              }
              (processed, summary)
            }
            processBatchGroup(remainingLines, currentState, future :: batchFutures)
          }
        } else {
          // Wait for completed futures
          val (completed, remaining) = batchFutures.partition(_.isCompleted)
          if (completed.nonEmpty) {
            val results = completed.map(Await.result(_, 1.minute))
            val newState = results.foldLeft(currentState) {
              case (state, (processed, (orig, disc, fin))) =>
                val inserted = oracleWriter.insertOrders(processed, tableName)
                logger.info(s"Processed batch: ${processed.length} orders, Inserted: $inserted")
                ProcessState(
                  processedCount = state.processedCount + processed.length,
                  totalOriginal = state.totalOriginal + orig,
                  totalDiscount = state.totalDiscount + disc,
                  totalFinal = state.totalFinal + fin,
                  totalInserted = state.totalInserted + inserted
                )
            }
            processBatchGroup(remainingLines, newState, remaining)
          } else {
            // No completed futures, wait a bit
            Thread.sleep(100)
            processBatchGroup(remainingLines, currentState, batchFutures)
          }
        }
      }

      processBatchGroup(lines, state, List.empty)
    }

    logger.info("Reading and processing CSV file in batches...")
    logger.info(s"Batch size: $batchSize records")

    // OPTIMIZATION 4: Use buffered source with larger buffer
    val source = Source.fromFile("src/main/resources/TRX10M.csv")(
      scala.io.Codec.UTF8
    )

    try {
      val lines = source.getLines()

      // Skip header
      val dataLines = if (lines.hasNext) {
        lines.next() // consume header
        lines
      } else {
        Iterator.empty
      }

      val finalState = processBatchesParallel(dataLines, batchSize, ProcessState(0, 0.0, 0.0, 0.0, 0))

      // Final summary
      logger.info("=== Summary ===")
      logger.info(f"Total Original Amount: $$${finalState.totalOriginal}%.2f")
      logger.info(f"Total Discount Amount: $$${finalState.totalDiscount}%.2f")
      logger.info(f"Total Final Amount: $$${finalState.totalFinal}%.2f")
      logger.info(s"Total Orders Processed: ${finalState.processedCount}")
      logger.info(s"Total Rows Inserted: ${finalState.totalInserted}")

      val totalDuration = System.currentTimeMillis() - startTime
      val avgThroughput = finalState.processedCount * 1000 / (totalDuration + 1)
      logger.info(s"TOTAL TIME:       ${totalDuration}ms (${totalDuration / 1000.0}s)")
      logger.info(s"AVG THROUGHPUT:   $avgThroughput orders/sec")
      logger.info("=== Order Discount System Completed Successfully ===")

    } finally {
      source.close()
      exit(0)
    }

  } catch {
    case e: Exception =>
      logger.error(s"Application failed: ${e.getMessage}")
      e.printStackTrace()
  }
}