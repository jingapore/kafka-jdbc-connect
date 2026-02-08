package dev.jingsong.kafkaJdbcConnector.sink

import dev.jingsong.kafkaJdbcConnector.sink.db.DbWriter
import dev.jingsong.kafkaJdbcConnector.BuildInfo
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.slf4j.LoggerFactory

class JdbcSinkTask : SinkTask() {
    private val log = LoggerFactory.getLogger(JdbcSinkTask::class.java)
    private lateinit var writer: DbWriter

    override fun start(props: Map<String, String>) {
        log.info("Starting JDBC Sink Task")
        try {
            val config = JdbcSinkConfig(props)
            // TODO: find the best dialect given the conn string
            writer = DbWriter(config, context, dev.jingsong.kafkaJdbcConnector.dialect.RedshiftDialect())
        } catch (e: Exception) {
            throw ConnectException("Failed to start JdbcSinkTask", e)
        }
    }

    override fun put(records: Collection<SinkRecord>) {
        if (records.isEmpty()) return
        writer.write(records)
    }

    override fun stop() {
        log.info("Stopping JDBC Sink Task")
        if (::writer.isInitialized) {
            writer.close()
        }
    }

    override fun preCommit(
        currentOffsets: Map<TopicPartition, OffsetAndMetadata>?
    ): Map<TopicPartition, OffsetAndMetadata> {
        writer.triggerFlushIfRequired()
        val flushedOffsets = writer.getCommittableOffsets() ?: emptyMap()
        if (flushedOffsets.isNotEmpty()) {
            log.debug("Committing offsets for {} partitions", flushedOffsets.size)
        }
        return flushedOffsets
    }

    override fun version(): String {
        return BuildInfo.version
    }
}