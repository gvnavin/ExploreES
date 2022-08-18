package com.gvnavin.test.esreader

data class SortParam(
    val key: String,
    val order: SortOrder,
    val script: Boolean = false,
    val params: Map<String, Any> = emptyMap()
) {
    enum class SortOrder {
        ASC, DESC
    }
}