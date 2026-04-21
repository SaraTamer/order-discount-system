import java.time.LocalDate
import java.time.format.DateTimeFormatter

class OrderParser {
  private val logger = new Logger()

  // CSV header columns
  val headers = List("timestamp", "product_name", "expiry_date", "quantity", "unit_price", "channel", "payment_method")

  // Date formatter for parsing expiry_date
  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  // Create Order from CSV line (comma-separated)
  def fromCsvLine(line: String): Either[String, Order] = {
    val parts = line.split(",").map(_.trim)

    if (parts.length != headers.length) {
      val error = s"Invalid CSV format: expected ${headers.length} fields, got ${parts.length}"
      logger.warn(error)
      return Left(error)
    }

    try {
      Right(Order(
        timestamp = parts(0),
        productName = parts(1),
        expiryDate = LocalDate.parse(parts(2), dateFormatter),
        quantity = parts(3).toInt,
        unitPrice = parts(4).toDouble,
        channel = parts(5),
        paymentMethod = parts(6)
      ))
    } catch {
      case e: Exception =>
        val error = s"Failed to parse line: ${e.getMessage}"
        logger.error(error)
        Left(error)
    }
  }

  // Parse multiple lines (skip header if present)
  def fromCsvLines(lines: List[String], hasHeader: Boolean = true): List[Either[String, Order]] = {
    val dataLines = if (hasHeader && lines.nonEmpty) lines.tail else lines
    dataLines.map(fromCsvLine)
  }

  // Get only successful parses
  def parseValidOrders(lines: List[String], hasHeader: Boolean = true): List[Order] = {
    val dataLines = if (hasHeader && lines.nonEmpty) lines.tail else lines
    logger.info(s"Parsing ${dataLines.length} data lines from CSV")
    val orders = dataLines.flatMap(line => fromCsvLine(line).toOption)
    logger.info(s"Successfully parsed ${orders.length} valid orders")
    orders
  }

}
