package jdbcConnector.dialect

class SqlExpressionBuilder(private val quoteChar: String) {
    private val sb = StringBuilder()

    fun append(text: String) = apply { sb.append(text) }

    fun appendIdentifier(name: String) = apply {
        sb.append(quoteChar).append(name).append(quoteChar)
    }

    fun appendList(items: List<String>, delimiter: String = ", ", transform: SqlExpressionBuilder.(String) -> Unit) =
        apply {
            items.forEachIndexed { index, item ->
                if (index > 0) append(delimiter)
                transform(item)
            }
        }

    override fun toString(): String = sb.toString()
}

fun buildSql(quoteChar: String = "\"", block: SqlExpressionBuilder.() -> Unit): String {
    return SqlExpressionBuilder(quoteChar).apply(block).toString()
}