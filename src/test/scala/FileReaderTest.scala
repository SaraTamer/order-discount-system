
object FileReaderTest {

  def main(args: Array[String]): Unit = {
    println("=== FileReader Test Suite ===\n")

    val fileReader = new FileReader()

    // Test 1: Read existing file
    testReadExistingFile(fileReader)

    println("\n=== Tests Complete ===")
  }

  def testReadExistingFile(fileReader: FileReader): Unit = {
    println("Test 1: Reading existing CSV file")
    val filePath = "D:\\iti\\24.FP with Scala\\order-discount-system\\src\\main\\resources\\TRX1000.csv"

    try {
      val orders = fileReader.readFromPath(filePath)

      if (orders.nonEmpty) {
        println(s"✓ SUCCESS: Read ${orders.length} lines")
        println(s"  First line: ${orders.head.take(100)}...")
        println(s"  Last line: ${orders.last.take(100)}...")
      } else {
        println("✗ FAILED: File is empty")
      }
    } catch {
      case e: Exception =>
        println(s"✗ FAILED: ${e.getMessage}")
    }
    println()
  }
  
}