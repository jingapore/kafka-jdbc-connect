package jdbcConnector.sink

import jdbcConnector.util.ConfigUtil
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement

class JdbcSinkTask : SinkTask() {

    private lateinit var jdbcUrl: String
    private lateinit var jdbcUser: String
    private lateinit var jdbcPassword: String
    private lateinit var driverClass: String

    private lateinit var schema: String
    private lateinit var table: String
    private lateinit var payloadColumn: String
    private var batchSize: Int = 200

    private var conn: Connection? = null
    private var stmt: PreparedStatement? = null
    private var buffered: Int = 0

    override fun version(): String = "0.1.0"

    override fun start(props: MutableMap<String, String>) {
        val p = props.toMap()

        jdbcUrl = ConfigUtil.required(p, ConfigKeys.JDBC_URL)
        jdbcUser = ConfigUtil.optional(p, ConfigKeys.JDBC_USER, "")
        jdbcPassword = ConfigUtil.optional(p, ConfigKeys.JDBC_PASSWORD, "")
        driverClass = ConfigUtil.optional(p, ConfigKeys.DRIVER_CLASS, "com.amazon.redshift.Driver")

        schema = ConfigUtil.optional(p, ConfigKeys.SCHEMA, "public")
        table = ConfigUtil.required(p, ConfigKeys.TABLE)
        payloadColumn = ConfigUtil.optional(p, ConfigKeys.PAYLOAD_COLUMN, "payload")
        batchSize = ConfigUtil.optionalInt(p, ConfigKeys.BATCH_SIZE, 200)

        // Force driver initialization (works for Redshift/Postgres/MySQL, etc.)
        Class.forName(driverClass)

        conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword).apply {
            autoCommit = false
        }

        stmt = conn!!.prepareStatement(insertSql(schema, table, payloadColumn))
    }

    override fun put(records: MutableCollection<SinkRecord>) {
        val c = conn ?: error("Task not started")
        val s = stmt ?: error("Task not started")

        for (r in records) {
            val payload = toPayloadString(r.value())
            s.setString(1, payload)
            s.addBatch()
            buffered++

            if (buffered >= batchSize) {
                flushBatch(c, s)
            }
        }

        // You can choose to flush only on flush(), but for a simple skeleton
        // it's ok to flush at end of put() to reduce latency.
        if (buffered > 0) {
            flushBatch(c, s)
        }
    }

    override fun flush(currentOffsets: MutableMap<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata>) {
        val c = conn ?: return
        val s = stmt ?: return
        if (buffered > 0) flushBatch(c, s)
    }

    override fun stop() {
        try {
            stmt?.close()
        } catch (_: Exception) {
        }
        try {
            conn?.close()
        } catch (_: Exception) {
        }
        stmt = null
        conn = null
    }

    private fun flushBatch(c: Connection, s: PreparedStatement) {
        try {
            s.executeBatch()
            c.commit()
            buffered = 0
        } catch (e: Exception) {
            try {
                c.rollback()
            } catch (_: Exception) {
            }
            throw e
        }
    }

    private fun insertSql(schema: String, table: String, col: String): String {
        // Keep identifiers quoted (Redshift/Postgres compatible). MySQL uses backticks;
        // we’ll abstract this later via a dialect.
        return """INSERT INTO "$schema"."$table"("$col") VALUES (?)"""
    }

    private fun toPayloadString(value: Any?): String {
        if (value == null) return "null"

        return when (value) {
            is String -> value
            is Map<*, *> -> {
                // Minimal “JSON-ish” representation without pulling in a JSON library.
                // Later you can use Jackson and proper JSON encoding.
                value.entries.joinToString(prefix = "{", postfix = "}") { (k, v) ->
                    "\"${k.toString()}\":\"${v?.toString() ?: "null"}\""
                }
            }

            is Struct -> {
                val schema = value.schema()
                schema.fields().joinToString(prefix = "{", postfix = "}") { f ->
                    val v = value.get(f)
                    "\"${f.name()}\":\"${v?.toString() ?: "null"}\""
                }
            }

            else -> value.toString()
        }
    }
}
