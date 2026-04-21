import java.io.{File, FileWriter, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.locks.ReentrantLock

class Logger(
              logDir: String = "logs",
              baseFileName: String = "app",
              maxFileSizeMB: Long = 100,  // Max file size in MB
              maxBackupFiles: Int = 5      // Keep 5 backup files
            ) {

  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
  private val lock = new ReentrantLock()
  private val logDirectory = new File(logDir)

  // Ensure log directory exists
  if (!logDirectory.exists()) {
    logDirectory.mkdirs()
  }

  private def getCurrentLogFile: File = {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    new File(logDirectory, s"${baseFileName}_$timestamp.log")
  }

  private def rotateIfNeeded(file: File): Unit = {
    if (file.exists() && file.length() > maxFileSizeMB * 1024 * 1024) {
      // Rotate the file
      for (i <- maxBackupFiles - 1 to 1 by -1) {
        val src = new File(logDirectory, s"${baseFileName}_${file.getName}.$i")
        val dest = new File(logDirectory, s"${baseFileName}_${file.getName}.${i + 1}")
        if (src.exists()) {
          src.renameTo(dest)
        }
      }
      val backup = new File(logDirectory, s"${baseFileName}_${file.getName}.1")
      file.renameTo(backup)
    }
  }

  private def writeLog(level: String, message: String): Unit = {
    val timestamp = LocalDateTime.now().format(dateFormatter)
    val logLine = f"$timestamp%-23s $level%-8s $message"

    lock.lock()
    try {
      // Only print WARN and ERROR to console (reduce console I/O)
      if (level == "ERROR" || level == "WARN") {
        println(logLine)
      }

      // Write to file with rotation
      val logFile = getCurrentLogFile
      rotateIfNeeded(logFile)

      val writer = new PrintWriter(new FileWriter(logFile, true))
      try {
        writer.println(logLine)
      } finally {
        writer.close()
      }
    } finally {
      lock.unlock()
    }
  }

  def info(message: String): Unit = writeLog("INFO", message)
  def warn(message: String): Unit = writeLog("WARN", message)
  def error(message: String): Unit = writeLog("ERROR", message)
  def debug(message: String): Unit = writeLog("DEBUG", message)  // Optional

  // Batch logging for better performance
  def logBatch(level: String, messages: List[String]): Unit = {
    val timestamp = LocalDateTime.now().format(dateFormatter)
    val logLines = messages.map(msg => f"$timestamp%-23s $level%-8s $msg")

    lock.lock()
    try {
      val logFile = getCurrentLogFile
      rotateIfNeeded(logFile)

      val writer = new PrintWriter(new FileWriter(logFile, true))
      try {
        logLines.foreach(writer.println)
      } finally {
        writer.close()
      }
    } finally {
      lock.unlock()
    }
  }
}