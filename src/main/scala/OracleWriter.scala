import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.annotation.tailrec

class OracleWriter(
                    url: String,
                    username: String,
                    password: String
                  ) {

  private val connection: Connection = DriverManager.getConnection(url, username, password)
  connection.setAutoCommit(false)
  val logger = Logger()

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
    } catch {
      case e: Exception =>
        if (e.getMessage.contains("ORA-00955")) {
          logger.warn(s"Table $tableName already exists")
        } else {
          logger.error(s"Error creating table: ${e.getMessage}")
        }
    }
  }

  def insertNewOrdersOnly(orders: List[ProcessedOrder], tableName: String = "orders_with_discounts"): Int = {
    if (orders.isEmpty) {
      logger.warn("No orders to insert")
      return 0
    }

    val checkSQL = s"SELECT COUNT(*) FROM $tableName WHERE timestamp = ?"
    val insertSQL =
      s"""
        INSERT INTO $tableName
        (timestamp, product_name, expiry_date, quantity, unit_price, channel, payment_method, discount_percentage, final_price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      """

    val checkStmt = connection.prepareStatement(checkSQL)
    val insertStmt = connection.prepareStatement(insertSQL)
    val batchSize = 5000

    try {
      // Filter out existing orders first (pure function)
      val newOrders = orders.filter { processedOrder =>
        checkStmt.setString(1, processedOrder.order.timestamp)
        val resultSet = checkStmt.executeQuery()
        resultSet.next()
        val exists = resultSet.getInt(1) > 0
        resultSet.close()
        !exists
      }

      val skippedCount = orders.length - newOrders.length
      if (skippedCount > 0) {
        logger.info(s"Skipped $skippedCount duplicate rows (already exist in database)")
      }

      // Insert using tail recursion
      @tailrec
      def insertBatch(remainingOrders: List[ProcessedOrder], currentBatch: List[ProcessedOrder], insertedSoFar: Int): Int = {
        (remainingOrders, currentBatch) match {
          // No more orders and empty batch - done
          case (Nil, Nil) => insertedSoFar

          // No more orders but batch has items - execute final batch
          case (Nil, batch) =>
            insertBatchToDatabase(insertStmt, batch)
            connection.commit()
            insertedSoFar + batch.length

          // Batch is full - execute it and continue with next batch
          case (next :: rest, batch) if batch.length + 1 >= batchSize =>
            val newBatch = batch :+ next
            insertBatchToDatabase(insertStmt, newBatch)
            connection.commit()
            insertBatch(rest, List.empty, insertedSoFar + newBatch.length)

          // Add to current batch and continue
          case (next :: rest, batch) =>
            insertBatch(rest, batch :+ next, insertedSoFar)
        }
      }

      // Helper to execute a batch
      def insertBatchToDatabase(stmt: PreparedStatement, batch: List[ProcessedOrder]): Unit = {
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

      val insertedCount = insertBatch(newOrders, List.empty, 0)
      insertedCount

    } catch {
      case e: Exception =>
        connection.rollback()
        logger.error(s"Error inserting rows: ${e.getMessage}")
        e.printStackTrace()
        0
    } finally {
      checkStmt.close()
      insertStmt.close()
    }
  }

  def close(): Unit = {
    if (connection != null && !connection.isClosed) {
      connection.commit()
      connection.close()
    }
  }
}