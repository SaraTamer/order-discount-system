import com.typesafe.config.{Config, ConfigFactory}

object DatabaseConfig {
  private val config: Config = ConfigFactory.load("db.conf")

  lazy val url: String = config.getString("oracle.url")
  lazy val user: String = config.getString("oracle.user")
  lazy val password: String = config.getString("oracle.password")
  lazy val tableName: String = config.getString("oracle.table")

  // Alternative with environment variable override
  lazy val urlFromEnv: String = sys.env.getOrElse("ORACLE_URL", url)
  lazy val userFromEnv: String = sys.env.getOrElse("ORACLE_USER", user)
  lazy val passwordFromEnv: String = sys.env.getOrElse("ORACLE_PASSWORD", password)
}