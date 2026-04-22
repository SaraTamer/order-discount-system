import java.sql.{Connection, DriverManager, PreparedStatement, Statement}
import scala.annotation.tailrec

class OracleWriter(
                    url: String,
                    username: String,
                    password: String
                  ) {

  // Keep one shared transactional connection for bulk insert performance.
  private val connection: Connection = DriverManager.getConnection(url, username, password)
  connection.setAutoCommit(false)
  val logger = Logger()

  // Truncate table function - removes all data efficiently
  def truncateTable(tableName: String = "orders_with_discounts"): Unit = {
    val truncateSQL = s"TRUNCATE TABLE $tableName"

    try {
      val stmt = connection.createStatement()
      stmt.execute(truncateSQL)
      connection.commit()
      logger.info(s"Table $tableName truncated successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Error truncating table $tableName: ${e.getMessage}")
        connection.rollback()
        throw e
    } finally {
      // No need to close Statement explicitly as it will be closed when connection is closed
    }
  }
  
  def createTableIfNotExists(tableName: String = "orders_with_discounts"): Unit = {
    val createTableSQL =
      s"""
        CREATE TABLE $tableName (
          timestamp VARCHAR2(50),
          product_name VARCHAR2(200),
          expiry_date DATE,
          quantity NUMBER(10),
          unit_price NUMBER(10,2),
          channel VARCHAR2(50),
          payment_method VARCHAR2(50),
          discount_percentage NUMBER(10,2),
          final_price NUMBER(10,2)
        )
      """

    try {
      val stmt = connection.createStatement()
      stmt.execute(createTableSQL)
      connection.commit()
      logger.info(s"Table $tableName created successfully")
    } catch {
      case e: Exception if e.getMessage.contains("ORA-00955") =>
        logger.warn(s"Table $tableName already exists")
      case e: Exception =>
        logger.error(s"Error creating table: ${e.getMessage}")
        connection.rollback()
        throw e
    }
  }

  // Optimized insert - direct insert without checking duplicates (much faster)
  def insertOrders(orders: List[ProcessedOrder], tableName: String = "orders_with_discounts"): Int = {
    if (orders.isEmpty) {
      logger.warn("No orders to insert")
      return 0
    }

    val insertSQL =
      s"""
        INSERT INTO $tableName
        (timestamp, product_name, expiry_date, quantity, unit_price, channel, payment_method, discount_percentage, final_price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      """

    val insertStmt = connection.prepareStatement(insertSQL)
    val batchSize = 10000 // Larger batch size for better performance

    try {
      // Tail-recursive batching avoids stack growth on very large datasets.
      @tailrec
      def insertBatch(remainingOrders: List[ProcessedOrder], currentBatch: List[ProcessedOrder], insertedSoFar: Int): Int = {
        (remainingOrders, currentBatch) match {
          case (Nil, Nil) => insertedSoFar
          case (Nil, batch) =>
            executeBatch(insertStmt, batch)
            connection.commit()
            insertedSoFar + batch.length
          case (next :: rest, batch) if batch.length + 1 >= batchSize =>
            val newBatch = batch :+ next
            executeBatch(insertStmt, newBatch)
            connection.commit()
            insertBatch(rest, List.empty, insertedSoFar + newBatch.length)
          case (next :: rest, batch) =>
            insertBatch(rest, next :: batch, insertedSoFar)
        }
      }

      // Binds order fields to JDBC parameters and sends a single DB batch.
      def executeBatch(stmt: PreparedStatement, batch: List[ProcessedOrder]): Unit = {
        batch.foreach { order =>
          stmt.setString(1, order.order.timestamp)
          stmt.setString(2, order.order.productName)
          stmt.setDate(3, java.sql.Date.valueOf(order.order.expiryDate))
          stmt.setInt(4, order.order.quantity)
          stmt.setDouble(5, order.order.unitPrice)
          stmt.setString(6, order.order.channel)
          stmt.setString(7, order.order.paymentMethod)
          stmt.setDouble(8, order.discountPercentage)
          stmt.setDouble(9, order.finalPrice)
          stmt.addBatch()
        }
        stmt.executeBatch()
      }

      val insertedCount = insertBatch(orders, List.empty, 0)
      logger.info(s"Successfully inserted $insertedCount orders into $tableName")
      insertedCount

    } catch {
      case e: Exception =>
        connection.rollback()
        logger.error(s"Error inserting rows: ${e.getMessage}")
        e.printStackTrace()
        0
    } finally {
      insertStmt.close()
    }
  }

  def close(): Unit = {
    if (connection != null && !connection.isClosed) {
      try {
        connection.commit()
        connection.close()
        logger.info("Database connection closed")
      } catch {
        case e: Exception => logger.error(s"Error closing connection: ${e.getMessage}")
      }
    }
  }
}