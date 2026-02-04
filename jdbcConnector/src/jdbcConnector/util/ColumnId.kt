package jdbcConnector.util

import io.confluent.connect.jdbc.util.ExpressionBuilder.Expressable

data class ColumnId(
    val tableId: TableId,
    val name: String,
    val alias: String? = null
) : Expressable {
    val aliasOrName: String = if (alias.isNullOrBlank()) name else alias
    override fun appendTo(builder: ExpressionBuilder, useQuotes: Boolean) {
        appendTo(builder, if (useQuotes) QuoteMethod.ALWAYS else QuoteMethod.NEVER)
    }

    override fun appendTo(builder: ExpressionBuilder, useQuotes: QuoteMethod) {
        tableId.let {
            builder.append(it)
            builder.appendIdentifierDelimiter()
        }
        builder.appendColumnName(name, useQuotes)
    }
}