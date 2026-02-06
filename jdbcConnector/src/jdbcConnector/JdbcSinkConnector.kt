package jdbc

import jdbcConnector.sink.JdbcSinkConfig
import jdbcConnector.sink.JdbcSinkTask
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

class JdbcSinkConnector : SinkConnector() {
    private var configProps: Map<String, String> = emptyMap()

    override fun start(props: Map<String, String>) {
        configProps = props
    }

    override fun taskClass(): Class<out Task> = JdbcSinkTask::class.java

    override fun taskConfigs(maxTasks: Int): List<Map<String, String>> {
        return List(maxTasks) { configProps }
    }

    override fun config(): ConfigDef = JdbcSinkConfig.CONFIG_DEF

    override fun stop() {

    }

    override fun version(): String {
        return "1.0.0"
    }

}