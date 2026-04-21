import java.sql.{Connection, DriverManager, PreparedStatement}
import java.time.LocalDate

class OracleWriter(
                    url: String,
                    username: String,
                    password: String
                  ) {

  private val connection: Connection = DriverManager.getConnection(url, username, password)
  connection.setAutoCommit(false)

  def createTableIfNotExists(tableName: String = "orders_with_discounts"): Unit = {
    val createTableSQL =
      s"""
        CREATE TABLE $tableName (
          timestamp VARCHAR2(50) PRIMARY KEY,
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
      println(s"Table $tableName created successfully")
    } catch {
      case e: Exception =>
        if (e.getMessage.contains("ORA-00955")) {
          println(s"Table $tableName already exists")
        } else if (e.getMessage.contains("ORA-01442")) {
          println(s"Table $tableName already has PRIMARY KEY constraint")
        } else {
          println(s"Error creating table: ${e.getMessage}")
        }
    }
  }
  
  def insertNewOrdersOnly(orders: List[ProcessedOrder], tableName: String = "orders_with_discounts"): Int = {
    if (orders.isEmpty) {
      println("No orders to insert")
      return 0
    }

    val checkSQL = s"SELECT COUNT(*) FROM $tableName WHERE timestamp = ?"
    val insertSQL =
      s"""
        INSERT INTO $tableName 
        (timestamp, product_name, expiry_date, quantity, unit_price, channel, payment_method, discount_percentage, final_price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      """

    var insertedCount = 0
    var skippedCount = 0
    val checkStmt = connection.prepareStatement(checkSQL)
    val insertStmt = connection.prepareStatement(insertSQL)

    try {
      orders.foreach { processedOrder =>
        val order = processedOrder.order

        // Check if timestamp already exists
        checkStmt.setString(1, order.timestamp)
        val resultSet = checkStmt.executeQuery()
        resultSet.next()
        val exists = resultSet.getInt(1) > 0
        resultSet.close()

        if (!exists) {
          // Insert new order
          insertStmt.setString(1, order.timestamp)
          insertStmt.setString(2, order.productName)
          insertStmt.setDate(3, java.sql.Date.valueOf(order.expiryDate))
          insertStmt.setInt(4, order.quantity)
          insertStmt.setDouble(5, order.unitPrice)
          insertStmt.setString(6, order.channel)
          insertStmt.setString(7, order.paymentMethod)
          insertStmt.setDouble(8, processedOrder.discountPercentage)
          insertStmt.setDouble(9, processedOrder.finalPrice)
          insertStmt.addBatch()
          insertedCount += 1
        } else {
          skippedCount += 1
          println(s"Skipping duplicate order with timestamp: ${order.timestamp}")
        }
      }

      if (insertedCount > 0) {
        insertStmt.executeBatch()
        connection.commit()
        println(s"Successfully inserted $insertedCount new rows into $tableName")
      }

      if (skippedCount > 0) {
        println(s"Skipped $skippedCount duplicate rows (already exist in database)")
      }

    } catch {
      case e: Exception =>
        connection.rollback()
        println(s"Error inserting rows: ${e.getMessage}")
        e.printStackTrace()
        insertedCount = 0
    } finally {
      checkStmt.close()
      insertStmt.close()
    }

    insertedCount
  }

  def close(): Unit = {
    if (connection != null && !connection.isClosed) {
      connection.commit()
      connection.close()
      println("Database connection closed")
    }
  }
}