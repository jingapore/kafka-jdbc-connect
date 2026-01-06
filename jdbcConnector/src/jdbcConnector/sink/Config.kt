package jdbcConnector.sink

import jdbcConnector.util.CommonConfigKeys

object SinkConfigKeys {
    const val JDBC_URL = CommonConfigKeys.JDBC_URL
    const val JDBC_USER = CommonConfigKeys.JDBC_USER
    const val JDBC_PASSWORD = CommonConfigKeys.JDBC_PASSWORD
    const val DRIVER_CLASS = CommonConfigKeys.DRIVER_CLASS
    const val SCHEMA = "schema"
    const val TABLE = "table"
    const val PAYLOAD_COLUMN = "payload.column"
    const val BATCH_SIZE = "batch.size"
}
