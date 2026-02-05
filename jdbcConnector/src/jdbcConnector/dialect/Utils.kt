package com.example.kafka.dialect

import org.apache.kafka.connect.data.Schema

interface DatabaseDialect {
    fun buildInsertStatement(table: TableId, fields: List<String>): String
    fun buildCreateTableStatement(table: TableId, schema: Schema): String
    fun buildAlterTableStatement(table: TableId, missingFields: List<String>, schema: Schema): List<String>
    fun typeOf(schema: Schema): String
    fun tableExists(connection: java.sql.Connection, table: TableId): Boolean
    fun getTableColumns(connection: java.sql.Connection, table: TableId): Set<String>
}

data class TableId(val schema: String, val name: String) {
    override fun toString() = "\"$schema\".\"$name\""
}

