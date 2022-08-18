package com.gvnavin.test.esreader

data class QueryParam(
    val key: String,
    val value: Any? = null,
    val operator: Operator = Operator.EQ,
    val isKeyword: Boolean = true
) {
    enum class Operator {
        EQ, LT, LTE, GT, GTE, IN, NOT_IN
    }
}