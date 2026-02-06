package jdbcConnector.sink

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

class JdbcSinkConfig(props: Map<String, String>) : AbstractConfig(CONFIG_DEF, props) {

    val connectionUrl = getString(CONNECTION_URL)!!
    val dbUser = getString(CONNECTION_USER)!!
    val dbPassword = getPassword(CONNECTION_PASSWORD)?.value()!!
    val targetSchema = getString(TARGET_SCHEMA) ?: "public"

    val flushSize = getInt(FLUSH_SIZE)
    val flushIntervalMs = getLong(FLUSH_INTERVAL_MS)

    val maxBufferHeapRatio = getDouble(MAX_BUFFER_HEAP_RATIO)

    val autoCreate = getBoolean(AUTO_CREATE)
    val autoEvolve = getBoolean(AUTO_EVOLVE)

    companion object {
        const val CONNECTION_URL = "connection.url"
        const val CONNECTION_USER = "connection.user"
        const val CONNECTION_PASSWORD = "connection.password"
        const val TARGET_SCHEMA = "target.schema"
        const val FLUSH_SIZE = "flush.size"
        const val FLUSH_INTERVAL_MS = "flush.interval.ms"
        const val MAX_BUFFER_HEAP_RATIO = "buffer.memory.heap.ratio"
        const val AUTO_CREATE = "auto.create"
        const val AUTO_EVOLVE = "auto.evolve"

        val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(CONNECTION_URL, Type.STRING, Importance.HIGH, "JDBC Connection URL")
            .define(CONNECTION_USER, Type.STRING, Importance.HIGH, "DB User")
            .define(CONNECTION_PASSWORD, Type.PASSWORD, Importance.HIGH, "DB Password")
            .define(TARGET_SCHEMA, Type.STRING, "public", Importance.HIGH, "Target Database Schema")
            .define(FLUSH_SIZE, Type.INT, 100, Importance.MEDIUM, "Flush threshold (records)")
            .define(FLUSH_INTERVAL_MS, Type.LONG, 10000L, Importance.MEDIUM, "Flush threshold (time)")
            .define(MAX_BUFFER_HEAP_RATIO, Type.DOUBLE, 0.05, Importance.LOW, "Max heap usage ratio for buffer")
            .define(AUTO_CREATE, Type.BOOLEAN, true, Importance.MEDIUM, "Auto create tables")
            .define(AUTO_EVOLVE, Type.BOOLEAN, true, Importance.MEDIUM, "Auto add missing columns")
    }
}