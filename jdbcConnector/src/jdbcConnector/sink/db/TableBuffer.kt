package jdbcConnector.sink.db

import com.example.kafka.dialect.DatabaseDialect
import com.example.kafka.dialect.TableId
import jdbcConnector.util.MemoryManager
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import java.sql.PreparedStatement

class TableBuffer(
    private val tableId: TableId,
    private val dialect: DatabaseDialect,
    private val memoryManager: MemoryManager
) {
    private val records = mutableListOf<SinkRecord>()
    var currentSizeBytes: Long = 0L
        private set

    val count: Int get() = records.size

    fun add(record: SinkRecord) {
        records.add(record)
        currentSizeBytes += memoryManager.estimateSize(record)
    }

    fun flush(connection: java.sql.Connection): Map<TopicPartition, Long> {
        if (records.isEmpty()) return emptyMap()
        val flushedOffsets = mutableMapOf<TopicPartition, Long>()
        records.forEach { record ->
            val tp = TopicPartition(record.topic(), record.kafkaPartition())
            val currentMax = flushedOffsets[tp] ?: -1L
            if (record.kafkaOffset() > currentMax) {
                flushedOffsets[tp] = record.kafkaOffset()
            }
        }

        // 2. Perform the JDBC Write
        // If this throws exception, we exit method, and records remain in buffer (correct for retry)
        executeBatch(connection)

        // 3. Clear Memory
        // We only reach here if JDBC write succeeded
        records.clear()
        currentSizeBytes = 0L

        return flushedOffsets
    }

    private fun bindRecord(stmt: PreparedStatement, record: SinkRecord, fields: List<String>) {
        val struct = record.value() as Struct
        fields.forEachIndexed { index, fieldName ->
            // In a real connector, use a comprehensive TypeConverter here
            val value = struct.get(fieldName)
            stmt.setObject(index + 1, value)
        }
    }

    private fun executeBatch(connection: java.sql.Connection) {
        val sample = records.first()
        val fields = sample.valueSchema().fields().map { it.name() }
        val sql = dialect.buildInsertStatement(tableId, fields)

        connection.prepareStatement(sql).use { stmt ->
            records.forEach { record ->
                bindRecord(stmt, record, fields)
                stmt.addBatch()
            }
            stmt.executeBatch()
        }
    }

    fun getFlushedOffsets(): Map<TopicPartition, Long> {
        val offsetMap = mutableMapOf<TopicPartition, Long>()

        records.forEach { record ->
            val tp = TopicPartition(record.topic(), record.kafkaPartition())
            val currentMax = offsetMap[tp] ?: -1L
            if (record.kafkaOffset() > currentMax) {
                offsetMap[tp] = record.kafkaOffset()
            }
        }

        records.clear()
        currentSizeBytes = 0L

        return offsetMap
    }
}