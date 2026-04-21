import java.time.LocalDate
import java.time.format.DateTimeFormatter

// Make it a singleton object to avoid repeated instantiation
object OrderParser {
  // CSV header columns
  val headers = List("timestamp", "product_name", "expiry_date", "quantity", "unit_price", "channel", "payment_method")

  // Date formatter for parsing expiry_date - thread-safe and reused
  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  // Pre-compile regex for faster splitting
  private val commaSplitter = ","

  // Create Order from CSV line - optimized for speed
  def fromCsvLine(line: String): Either[String, Order] = {
    // Use indexOf for faster parsing (avoids regex overhead)
    val parts = line.split(commaSplitter, -1)

    if (parts.length != headers.length) {
      Left(s"Invalid CSV format: expected ${headers.length} fields, got ${parts.length}")
    } else {
      try {
        // Trim only when necessary (micro-optimization)
        Right(Order(
          timestamp = if (parts(0).charAt(0) == ' ') parts(0).trim else parts(0),
          productName = if (parts(1).charAt(0) == ' ') parts(1).trim else parts(1),
          expiryDate = LocalDate.parse(parts(2), dateFormatter),
          quantity = parts(3).toInt,
          unitPrice = parts(4).toDouble,
          channel = if (parts(5).charAt(0) == ' ') parts(5).trim else parts(5),
          paymentMethod = if (parts(6).charAt(0) == ' ') parts(6).trim else parts(6)
        ))
      } catch {
        case _: Exception => Left(s"Failed to parse line")
      }
    }
  }
}