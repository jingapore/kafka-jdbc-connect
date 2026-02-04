package jdbcConnector.util

import org.apache.kafka.common.config.ConfigDef

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

data class ConfigField<T>(val name: String, val default_val: T, val doc: String);