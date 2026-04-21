import java.time.LocalDate

case class Order(
                  timestamp: String,
                  productName: String,
                  expiryDate: LocalDate,
                  quantity: Int,
                  unitPrice: Double,
                  channel: String,
                  paymentMethod: String
                )

case class ProcessedOrder(
                           order: Order,
                           discountPercentage: Double,
                           discountAmount: Double,
                           finalPrice: Double
                         ) {
  // Flatten all fields for database insertion
  def toRow: Seq[Any] = Seq(
    order.timestamp,
    order.productName,
    order.expiryDate,
    order.quantity,
    order.unitPrice,
    order.channel,
    order.paymentMethod,
    discountPercentage,
    finalPrice
  )
}