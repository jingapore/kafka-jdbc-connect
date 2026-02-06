package jdbcConnector.util

import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.LoggerFactory

class MemoryManager(heapRatio: Double) {
    private val log = LoggerFactory.getLogger(MemoryManager::class.java)

    val maxBufferBytes: Long = (Runtime.getRuntime().maxMemory() * heapRatio).toLong()

    init {
        log.info("MemoryManager initialized. Max Buffer: ${maxBufferBytes / 1024 / 1024} MB")
    }

    fun estimateSize(record: SinkRecord): Long {
        val keySize = sizeOf(record.key())
        val valueSize = sizeOf(record.value())
        return keySize + valueSize + 64L
    }

    private fun sizeOf(obj: Any?): Long {
        return when (obj) {
            null -> 0L
            is String -> obj.length * 2L
            is ByteArray -> obj.size.toLong()
            is Number -> 8L
            else -> 128L
        }
    }

    fun isJvmUnderStress(): Boolean {
        val free =
            Runtime.getRuntime().freeMemory() + (Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory())
        return (free.toDouble() / Runtime.getRuntime().maxMemory()) < 0.05
    }
}