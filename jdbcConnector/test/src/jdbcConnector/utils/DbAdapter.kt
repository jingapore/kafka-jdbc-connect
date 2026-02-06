package jdbcConnector.utils
import java.time.Duration
import java.util.concurrent.TimeoutException

interface DbAdapter {
    val name: String
    val driverClassName: String
    val schema: String
    fun createTable(table: String)
    fun dropTable(table: String)
    fun rowCount(table: String): Int
    fun awaitRowCount(table: String, expected: Int, timeout: Duration) {
        val deadline = System.nanoTime() + timeout.toNanos()
        var last = -1
        while (System.nanoTime() < deadline) {
            last = rowCount(table)
            if (last >= expected) return
            Thread.sleep(500)
        }
        throw AssertionError("[$name] Timed out waiting for $expected rows; lastSeen=$last table=$schema.$table")
    }

    fun awaitTableToExist(table: String, timeout: Duration) {
        val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos()
        while (System.nanoTime() < deadline) {
            try {
                awaitRowCount(table, 0, Duration.ofMillis(1000))
                return
            } catch (_: Exception) {
                Thread.sleep(1000)
            }
        }
        throw TimeoutException("[$name] Timed out waiting for table=$schema.$table")
    }

    fun connectorConfig(table: String): Map<String, String>
}
