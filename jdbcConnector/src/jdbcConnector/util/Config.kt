package jdbcConnector.util

object CommonConfigKeys {
    const val JDBC_URL = "jdbc.url"
    const val JDBC_USER = "jdbc.user"
    const val JDBC_PASSWORD = "jdbc.password"
    const val DRIVER_CLASS = "driver.class"
}

object ConfigUtil {
    fun required(props: Map<String, String>, key: String): String =
        props[key]?.takeIf { it.isNotBlank() }
            ?: throw IllegalArgumentException("Missing required config: $key")

    fun optional(props: Map<String, String>, key: String, default: String): String =
        props[key]?.takeIf { it.isNotBlank() } ?: default

    fun optionalInt(props: Map<String, String>, key: String, default: Int): Int =
        props[key]?.toIntOrNull() ?: default
}
