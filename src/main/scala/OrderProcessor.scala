case class OrderProcessor(
                                rules: List[DiscountRule],
                              ){
  private val logger = new Logger()

  def calculateAverageOfTopTwoDiscounts(order: Order): Double = {
    val applicableDiscounts = this.rules
      .filter(_.isQualified(order)) // Keep only rules that apply
      .map(_.discountAmount(order)) // Get their discount values
      .sorted(Ordering[Double].reverse) // Sort descending

    val discount = {
      if (applicableDiscounts.isEmpty) 0.0
      else if (applicableDiscounts.length == 1) applicableDiscounts.head
      else applicableDiscounts.take(2).sum / 2.0
    }
    if (applicableDiscounts.nonEmpty) {
      logger.info(s"Order ${order.timestamp}: discounts=${applicableDiscounts.mkString(",")} → result=$discount")
    }
    discount
  }

  def calculateFinalPrice(order: Order): ProcessedOrder = {
    val discountPercent = calculateAverageOfTopTwoDiscounts(order)
    val originalPrice = order.unitPrice * order.quantity
    val discountAmount = discountPercent * originalPrice
    val finalPrice = originalPrice - discountAmount
    val finalPriceRounded = f"$finalPrice%.2f".toDouble

    logger.info(s"Order: ${order.productName}, Qty: ${order.quantity}, Original: $$$originalPrice, Discount: $$$discountAmount, Final: $$$finalPriceRounded")

    ProcessedOrder(order, discountPercent, discountAmount, finalPrice)
  }

}
