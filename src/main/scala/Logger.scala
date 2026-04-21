import java.io.{File, FileWriter, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class Logger(logFilePath: String = "logs/app.log") {

  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
  private val logFile = new File(logFilePath)

  // Ensure log directory exists
  logFile.getParentFile.mkdirs()

  private def writeLog(level: String, message: String): Unit = {
    val timestamp = LocalDateTime.now().format(dateFormatter)
    val logLine = f"$timestamp%-23s $level%-8s $message"

    // Print to console
    println(logLine)

    // Write to file
    val writer = new PrintWriter(new FileWriter(logFile, true))
    try {
      writer.println(logLine)
    } finally {
      writer.close()
    }
  }

  def info(message: String): Unit = writeLog("INFO", message)
  def warn(message: String): Unit = writeLog("WARN", message)
  def error(message: String): Unit = writeLog("ERROR", message)
}