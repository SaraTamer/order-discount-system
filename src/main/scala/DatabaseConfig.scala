import com.typesafe.config.{Config, ConfigFactory}

object DatabaseConfig {
  // Load Oracle settings from db.conf once and reuse lazily.
  private val config: Config = ConfigFactory.load("db.conf")

  lazy val url: String = config.getString("oracle.url")
  lazy val user: String = config.getString("oracle.user")
  lazy val password: String = config.getString("oracle.password")
  lazy val tableName: String = config.getString("oracle.table")
}