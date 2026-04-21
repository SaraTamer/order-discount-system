import scala.io.{Codec, Source}

class FileReader {
  private val logger = new Logger()
  
  def readFromPath(filePath: String, codec: String = Codec.default.toString): List[String] = {
    logger.info(s"Reading file from path: $filePath")
    try {
      val lines = Source.fromFile(filePath, codec).getLines().toList
      logger.info(s"Successfully read ${lines.length} lines from file")
      lines
    } catch {
      case e: Exception =>
        logger.error(s"Failed to read file: ${e.getMessage}")
        throw e
    }
  }
}
