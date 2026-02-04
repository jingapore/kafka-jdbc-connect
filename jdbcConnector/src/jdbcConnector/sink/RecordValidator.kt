package jdbcConnector.sink

import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.data.Schema

fun interface RecordValidator {
    fun validate(record: SinkRecord)

    companion object {
        // A simple "do nothing" validator
        val NO_OP = RecordValidator { }

        fun create(config: SinkConfig): RecordValidator {
            val requiresKey = requiresKey(config)
            val requiresValue = requiresValue(config)

            var validator: RecordValidator = NO_OP

            // Compose logic using 'and' extension
            when (config.pkMode) {
                PkMode.RECORD_KEY -> validator = validator and requiresKey
                PkMode.RECORD_VALUE, PkMode.NONE -> validator = validator and requiresValue
                PkMode.KAFKA -> { /* no-op */ }
            }

            validator = if (config.deleteEnabled) {
                validator and requiresKey
            } else {
                validator and requiresValue
            }

            return validator
        }

        private fun requiresValue(config: JdbcSinkConfig) = RecordValidator { record ->
            val schema = record.valueSchema()
            if (record.value() == null || schema?.type() != Schema.Type.STRUCT) {
                throw ConnectException("Sink connector ${config.connectorName()} requires non-null Struct value...")
            }
        }

        private fun requiresKey(config: JdbcSinkConfig) = RecordValidator { record ->
            val schema = record.keySchema()
            if (record.key() == null || schema == null || (!schema.type().isPrimitive && schema.type() != Schema.Type.STRUCT)) {
                throw ConnectException("Sink connector ${config.connectorName()} requires non-null Key...")
            }
        }
    }
}

infix fun RecordValidator.and(other: RecordValidator?): RecordValidator {
    if (other == null || other === RecordValidator.NO_OP || other === this) return this
    if (this === RecordValidator.NO_OP) return other

    return RecordValidator { record ->
        this.validate(record)
        other.validate(record)
    }
}