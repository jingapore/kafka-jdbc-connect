package jdbcConnector.dialect

import com.example.kafka.dialect.DatabaseDialect
import com.example.kafka.dialect.TableId
import org.apache.kafka.connect.data.Schema

class RedshiftDialect : DatabaseDialect {
    override fun buildInsertStatement(table: TableId, fields: List<String>): String = buildSql {
        append("INSERT INTO ").append(table.toString()).append(" (")
        appendList(fields) { appendIdentifier(it) }
        append(") VALUES (")
        appendList(fields) { append("?") }
        append(")")
    }

    override fun buildCreateTableStatement(table: TableId, schema: Schema): String = buildSql {
        append("CREATE TABLE ").append(table.toString()).append(" (")
        appendList(schema.fields()) { field ->
            appendIdentifier(field.name()).append(" ").append(typeOf(field.schema()))
        }
        append(")")
    }

    override fun buildAlterTableStatement(table: TableId, missingFields: List<String>, schema: Schema): List<String> {
        return missingFields.map { fieldName ->
            val fieldSchema = schema.field(fieldName).schema()
            buildSql {
                append("ALTER TABLE ").append(table.toString())
                append(" ADD COLUMN ")
                appendIdentifier(fieldName).append(" ").append(typeOf(fieldSchema))
            }
        }
    }

    override fun typeOf(schema: Schema): String {
        return when (schema.type()) {
            Schema.Type.INT8, Schema.Type.INT16 -> "SMALLINT"
            Schema.Type.INT32 -> "INTEGER"
            Schema.Type.INT64 -> "BIGINT"
            Schema.Type.FLOAT32 -> "FLOAT4"
            Schema.Type.FLOAT64 -> "FLOAT8"
            Schema.Type.BOOLEAN -> "BOOLEAN"
            Schema.Type.STRING -> "VARCHAR(MAX)"
            Schema.Type.BYTES -> "VARBYTE"
            Schema.Type.ARRAY -> "SUPER"
            Schema.Type.MAP -> "SUPER"
            Schema.Type.STRUCT -> "SUPER"
        }
    }

    override fun tableExists(connection: java.sql.Connection, table: TableId): Boolean {
        val meta = connection.metaData
        val rs = meta.getTables(null, table.schema, table.name, null)
        return rs.next()
    }

    override fun getTableColumns(connection: java.sql.Connection, table: TableId): Set<String> {
        val columns = mutableSetOf<String>()
        val rs = connection.metaData.getColumns(null, table.schema, table.name, null)
        while (rs.next()) {
            columns.add(rs.getString("COLUMN_NAME"))
        }
        return columns
    }
}