import java.time.LocalDate
import java.time.format.DateTimeFormatter

class OrderParser {
  // CSV header columns
  val headers = List("timestamp", "product_name", "expiry_date", "quantity", "unit_price", "channel", "payment_method")

  // Date formatter for parsing expiry_date - thread-safe
  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  // Create Order from CSV line - no logging for performance
  def fromCsvLine(line: String): Either[String, Order] = {
    val parts = line.split(",", -1).map(_.trim)

    if (parts.length != headers.length) {
      Left(s"Invalid CSV format: expected ${headers.length} fields, got ${parts.length}")
    } else {
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
        case _: Exception => Left(s"Failed to parse line")
      }
    }
  }
}