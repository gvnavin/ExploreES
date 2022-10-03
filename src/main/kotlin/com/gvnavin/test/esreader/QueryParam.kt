package com.gvnavin.test.esreader

data class QueryParam(
    val key: String,
    val value: Any? = null,
    val operator: Operator = Operator.EQ,
    val isKeyword: Boolean = true,
    //for matchPhrasePrefix
    val slop: Int = 0,
    val maxExpansion: Int = 0
) {
    enum class Operator {
        EQ, LT, LTE, GT, GTE, IN, NOT_IN
    }
}