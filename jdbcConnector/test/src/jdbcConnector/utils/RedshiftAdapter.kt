package jdbcConnector.utils

import java.sql.DriverManager
import java.util.UUID

class RedshiftAdapter : DbAdapter {
    override val name: String = "redshift"
    override val driverClassName: String = "com.amazon.redshift.Driver"
    override val schema: String = (System.getenv("REDSHIFT_SCHEMA")?.takeIf { it.isNotBlank() } ?: "public")

    private val jdbcUrl: String = env("REDSHIFT_JDBC_URL")
    private val user: String = env("REDSHIFT_USER")
    private val password: String = env("REDSHIFT_PASSWORD")

    override fun newTableName(): String =
        "it_kc_${UUID.randomUUID().toString().replace("-", "")}"

    override fun createTable(table: String) {
        Class.forName(driverClassName)
        DriverManager.getConnection(jdbcUrl, user, password).use { c ->
            c.createStatement().use { st ->
                st.execute("""CREATE SCHEMA IF NOT EXISTS "$schema";""")
                st.execute(
                    """CREATE TABLE "$schema"."$table" (k VARCHAR(256), v VARCHAR(65535));"""
                )
            }
        }
    }

    override fun dropTable(table: String) {
        DriverManager.getConnection(jdbcUrl, user, password).use { c ->
            c.createStatement().use { st ->
                st.execute("""DROP TABLE IF EXISTS "$schema"."$table";""")
            }
        }
    }

    override fun rowCount(table: String): Int {
        DriverManager.getConnection(jdbcUrl, user, password).use { c ->
            c.prepareStatement("""SELECT COUNT(*) FROM "$schema"."$table";""").use { ps ->
                ps.executeQuery().use { rs ->
                    rs.next()
                    return rs.getInt(1)
                }
            }
        }
    }

    override fun connectorConfig(table: String): Map<String, String> = mapOf(
        "jdbc.url" to jdbcUrl,
        "jdbc.user" to user,
        "jdbc.password" to password,
        "schema" to schema,
        "table" to table
    )

    private fun env(k: String): String =
        System.getenv(k)?.takeIf { it.isNotBlank() }
            ?: throw IllegalStateException("Missing required env var: $k")
}