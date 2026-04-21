object orderProcessing extends App {

  val logger = new Logger()

  logger.info("=== Order Discount System Started ===")

  try {
    logger.info("Reading CSV file...")
    val lines = FileReader().readFromPath("D:\\iti\\24.FP with Scala\\order-discount-system\\src\\main\\resources\\TRX1000.csv")

    logger.info("Parsing orders...")
    val orders = OrderParser().parseValidOrders(lines)

    logger.info("Building discount rules...")
    val rules = RulesBuilder().getRules

    logger.info("Calculating discounts...")
    val orderProcessor = OrderProcessor(rules)

    logger.info("Processing orders...")
    val processedOrders = orders
      .map(orderProcessor.processOrder)

    logger.info("=== Results ===")
    processedOrders.zipWithIndex.foreach { case (price, idx) =>
      logger.info(s"Order ${idx + 1}: Final price = $$$price")
    }

    logger.info(s"Total orders processed: ${orders.length}")
    logger.info("=== Order Discount System Completed Successfully ===")

    // 6. Write to Oracle database
    logger.info("Connecting to Oracle database...")
    import DatabaseConfig._
    val oracleWriter = new OracleWriter(url, user, password)

    logger.info(s"Creating table $tableName if not exists...")
    oracleWriter.createTableIfNotExists(tableName)

    logger.info(s"Inserting ${processedOrders.length} processed orders into Oracle...")
    val insertedCount = oracleWriter.insertNewOrdersOnly(processedOrders, tableName)

    logger.info(s"Successfully inserted $insertedCount rows into Oracle database")

    // 7. Summary
    val totalOriginal = processedOrders.map(order => order.order.unitPrice * order.order.quantity).sum
    val totalDiscount = processedOrders.map(_.discountAmount).sum
    val totalFinal = processedOrders.map(_.finalPrice).sum

    logger.info("=== Summary ===")
    logger.info(f"Total Original Amount: $$${totalOriginal}%.2f")
    logger.info(f"Total Discount Amount: $$${totalDiscount}%.2f")
    logger.info(f"Total Final Amount: $$${totalFinal}%.2f")
    logger.info("=== Order Discount System Completed Successfully ===")


  } catch {
    case e: Exception =>
      logger.error(s"Application failed: ${e.getMessage}")
      e.printStackTrace()
  }
}