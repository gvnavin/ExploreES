package com.gvnavin.test.esreader

data class ElasticSearchResult(
    val total: Long,
    val start: Int,
    val size: Int,
    val resultAsListMap: List<Map<String, Any>>,
    val resultAsListString: List<String>
)