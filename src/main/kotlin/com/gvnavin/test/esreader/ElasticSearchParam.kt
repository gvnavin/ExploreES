package com.gvnavin.test.esreader

import com.gvnavin.test.esreader.QueryParam
import com.gvnavin.test.esreader.SortParam

data class ElasticSearchParam(
    val queryId: Any,
    val queryParams: List<QueryParam> = emptyList(),
    val orQueryParams: List<List<QueryParam>> = emptyList(),
    val rangeParams: RangeParam = RangeParam(),
    val uniqueParam: String? = null,
    val sortParams: List<SortParam> = emptyList(),
    val from: Int = 0,
    val size: Int = 10
)