package jdbcConnector.sink.db

import com.example.kafka.dialect.DatabaseDialect
import com.example.kafka.dialect.TableId
import jdbcConnector.sink.JdbcSinkConfig
import jdbcConnector.util.MemoryManager
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTaskContext
import org.slf4j.LoggerFactory
import java.sql.DriverManager
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

class DbWriter(
    private val config: JdbcSinkConfig, private val context: SinkTaskContext, private val dialect: DatabaseDialect
) {
    private val log = LoggerFactory.getLogger(DbWriter::class.java)

    private val memoryManager = MemoryManager(config.maxBufferHeapRatio)
    private val buffers = mutableMapOf<TableId, TableBuffer>()
    private val pausedPartitions = mutableSetOf<TopicPartition>()

    private val knownTables = mutableSetOf<TableId>()

    private var lastFlushTime = Instant.now()
    private val connection = DriverManager.getConnection(config.connectionUrl, config.dbUser, config.dbPassword)
    private val committableOffsets = ConcurrentHashMap<TopicPartition, Long>()

    init {
        connection.autoCommit = false
    }

    fun write(records: Collection<SinkRecord>) {
        records.forEach { record ->
            val tableId = TableId(config.targetSchema, record.topic())
            ensureTableReady(tableId, record)
            val buffer = buffers.getOrPut(tableId) { TableBuffer(tableId, dialect, memoryManager) }
            buffer.add(record)
            checkBackpressure(record)
        }

        triggerFlushIfRequired()
    }

    private fun ensureTableReady(tableId: TableId, record: SinkRecord) {
        if (tableId in knownTables) return

        if (!dialect.tableExists(connection, tableId)) {
            if (config.autoCreate) {
                val sql = dialect.buildCreateTableStatement(tableId, record.valueSchema())
                executeDdl(sql)
                log.info("Created table $tableId")
            } else {
                throw RuntimeException("Table $tableId does not exist and auto-create is off")
            }
        } else if (config.autoEvolve) {
            val dbColumns = dialect.getTableColumns(connection, tableId)
            val recordFields = record.valueSchema().fields().map { it.name() }
            val missing = recordFields.filter { !dbColumns.contains(it) }

            if (missing.isNotEmpty()) {
                val sqls = dialect.buildAlterTableStatement(tableId, missing, record.valueSchema())
                sqls.forEach { executeDdl(it) }
                log.info("Altered table $tableId adding columns $missing")
            }
        }
        knownTables.add(tableId)
    }

    fun triggerFlushIfRequired() {
        if (shouldFlush()) {
            flushAll()
        }
    }

    private fun executeDdl(sql: String) {
        connection.createStatement().use { it.execute(sql) }
        connection.commit()
    }

    private fun checkBackpressure(record: SinkRecord) {
        val totalBytes = buffers.values.sumOf { it.currentSizeBytes }
        val tp = TopicPartition(record.topic(), record.kafkaPartition())

        if (totalBytes >= memoryManager.maxBufferBytes || memoryManager.isJvmUnderStress()) {
            if (pausedPartitions.add(tp)) {
                log.warn("Backpressure applied. Usage: ${totalBytes / 1024}KB. Pausing $tp")
                context.pause(tp)
            }
        }
    }

    private fun shouldFlush(): Boolean {
        if (config.flushSize <= 0 && config.flushIntervalMs <= 0L) return true

        val totalRecords = buffers.values.sumOf { it.count }
        val timeSinceFlush = Duration.between(lastFlushTime, Instant.now()).toMillis()

        val hitCount = config.flushSize > 0 && totalRecords >= config.flushSize
        val hitTime = config.flushIntervalMs > 0 && timeSinceFlush >= config.flushIntervalMs

        return hitCount || hitTime
    }

    private fun flushAll() {
        try {
            buffers.values.forEach { buffer ->
                val batchOffsets = buffer.flush(connection)
                batchOffsets.forEach { (tp, offset) ->
                    committableOffsets.merge(tp, offset) { old, new -> kotlin.math.max(old, new) }
                }
            }

            connection.commit()
            lastFlushTime = Instant.now()

            if (pausedPartitions.isNotEmpty()) {
                log.info("Buffer flushed. Resuming partitions: $pausedPartitions")
                context.resume(*pausedPartitions.toTypedArray())
                pausedPartitions.clear()
            }
        } catch (e: Exception) {
            log.error("Flush failed, rolling back", e)
            connection.rollback()
            throw e
        }
    }

    fun getCommittableOffsets(): Map<TopicPartition, OffsetAndMetadata> {
        val offsetsToCommit = mutableMapOf<TopicPartition, OffsetAndMetadata>()
        val iterator = committableOffsets.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            val tp = entry.key
            val maxOffset = entry.value
            // TODO: write test case to ensure that kafka expects maxOffset + 1
            offsetsToCommit[tp] = OffsetAndMetadata(maxOffset + 1)
            iterator.remove()
        }
        return offsetsToCommit
    }

    fun close() {
        runCatching { connection.close() }
    }
}