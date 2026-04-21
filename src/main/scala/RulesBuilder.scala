import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

case class DiscountRule(
                         name: String,
                         isQualified: Order => Boolean,
                         discountAmount: Order => Double
                       )

class RulesBuilder {

  private val logger = Logger()

  private def isCheese(o: Order): Boolean = {
    val pattern = "^cheese.*".r
    pattern.matches(o.productName.toLowerCase)
  }
  private def cheeseDiscount(o: Order): Double = {
    0.1
  }
  private def isWine(o:Order): Boolean = {
    val pattern = "^wine .*".r
    pattern.matches(o.productName.toLowerCase)
  }
  private def wineDiscount(o: Order): Double = {
    0.05
  }
  private def isSpecialDate(o: Order): Boolean ={
    val month = o.timestamp.slice(5, 7)
    val day = o.timestamp.slice(8, 10)

    month == "03" && day == "23"
  }
  private def specialDateDiscount(o: Order): Double = {
    0.5
  }
  private def isHighQuantity(o: Order): Boolean = {
    o.quantity >= 6
  }
  private def highQuantityDiscount(o: Order): Double = {
    if(o.quantity <= 9) 0.05
    else if(o.quantity <= 14) 0.07
    else 0.1
  }
  private def isAboutToExpire(o: Order): Boolean = {
    val orderDateString = o.timestamp.split('T')(0)
    val orderDate = LocalDate.parse(orderDateString, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val daysToExpire = ChronoUnit.DAYS.between(orderDate, o.expiryDate)

    daysToExpire > 0 && daysToExpire < 30
  }
  private def aboutToExpireDiscount(o: Order): Double = {

    val orderDateString = o.timestamp.split('T')(0)
    val orderDate = LocalDate.parse(orderDateString, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val daysToExpire = ChronoUnit.DAYS.between(orderDate, o.expiryDate)

    (30 - daysToExpire) / 100.0
  }
  private def isThroughApp(o: Order): Boolean = {
    o.channel.toLowerCase == "app"
  }
  private def throughAppDiscount(o: Order): Double = {
    val remainder = o.quantity % 5
    if(remainder == 0) o.quantity / 100
    else {
      (5 - remainder + o.quantity) / 100
    }
  }
  private def isVisa(o: Order): Boolean = {
    o.paymentMethod.toLowerCase == "visa"
  }
  private def visaDiscount(o: Order): Double = {
    0.5
  }
  def getRules: List[DiscountRule] = {
    val rules = List(
      DiscountRule("Cheese Discount", isCheese, cheeseDiscount),
      DiscountRule("Wine Discount", isWine, wineDiscount),
      DiscountRule("Special Date Discount", isSpecialDate, specialDateDiscount),
      DiscountRule("High Quantity Discount", isHighQuantity, highQuantityDiscount),
      DiscountRule("About to Expire Discount", isAboutToExpire, aboutToExpireDiscount),
      DiscountRule("Sale Through App Discount", isThroughApp, throughAppDiscount),
      DiscountRule("Visa Payment Discount", isVisa, visaDiscount)
    )
    logger.info(s"Built ${rules.size} discount rules")
    rules
  }

}
