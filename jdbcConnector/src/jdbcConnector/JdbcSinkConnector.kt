package jdbcConnector

import jdbcConnector.sink.ConfigKeys
import jdbcConnector.sink.JdbcSinkTask
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

class JdbcSinkConnector : SinkConnector() {

    private lateinit var props: Map<String, String>

    override fun start(props: MutableMap<String, String>) {
        // Kafka Connect gives a mutable map; store an immutable copy
        this.props = props.toMap()
    }

    override fun taskClass(): Class<out Task> = JdbcSinkTask::class.java

    override fun taskConfigs(maxTasks: Int): List<Map<String, String>> {
        // For now, every task gets same config.
        // Later, you can partition by topic/partition or use transforms.
        val cfgs = ArrayList<Map<String, String>>(maxTasks)
        repeat(maxTasks) { cfgs.add(props) }
        return cfgs
    }

    override fun stop() {
        // No-op; tasks handle their own resources
    }

    override fun config(): ConfigDef {
        return ConfigDef()
            .define(ConfigKeys.JDBC_URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "JDBC URL")
            .define(ConfigKeys.JDBC_USER, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "JDBC user")
            .define(ConfigKeys.JDBC_PASSWORD, ConfigDef.Type.PASSWORD, "", ConfigDef.Importance.MEDIUM, "JDBC password")
            .define(
                ConfigKeys.DRIVER_CLASS,
                ConfigDef.Type.STRING,
                "com.amazon.redshift.Driver", // default to Redshift; override for postgres/mysql later
                ConfigDef.Importance.MEDIUM,
                "JDBC driver class (e.g. com.amazon.redshift.Driver, org.postgresql.Driver, com.mysql.cj.jdbc.Driver)"
            )
            .define(ConfigKeys.SCHEMA, ConfigDef.Type.STRING, "public", ConfigDef.Importance.MEDIUM, "Target schema")
            .define(ConfigKeys.TABLE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Target table")
            .define(
                ConfigKeys.PAYLOAD_COLUMN,
                ConfigDef.Type.STRING,
                "payload",
                ConfigDef.Importance.MEDIUM,
                "Column to store payload"
            )
            .define(
                ConfigKeys.BATCH_SIZE,
                ConfigDef.Type.INT,
                200,
                ConfigDef.Importance.LOW,
                "Max records per batch/transaction"
            )
    }

    override fun version(): String = "0.1.0"
}
