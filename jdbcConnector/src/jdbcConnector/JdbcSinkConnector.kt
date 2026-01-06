package jdbcConnector

import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

class JdbcSinkConnector : SinkConnector() {
    override fun start(props: Map<String?, String?>?) {
        TODO("Not yet implemented")
    }

    override fun taskClass(): Class<out Task?>? {
        TODO("Not yet implemented")
    }

    override fun taskConfigs(maxTasks: Int): List<Map<String?, String?>?>? {
        final configs = ArrayList(maxTasks)
    }

    override fun stop() {
        TODO("Not yet implemented")
    }

    override fun config(): org.apache.kafka.common.config.ConfigDef {
        TODO("Not yet implemented")
    }

    override fun version(): String? {
        TODO("Not yet implemented")
    }

}