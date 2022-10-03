package com.gvnavin.test

import com.gvnavin.test.esreader.ElasticSearchParam
import com.gvnavin.test.esreader.ElasticSearchResult
import com.gvnavin.test.esreader.QueryParam
import com.gvnavin.test.esreader.RangeParam


fun main(args: Array<String>) {

    val esDao = ESDao("user")

    val searchList: ArrayList<ElasticSearchParam> = ArrayList()

    searchList.add(
        ElasticSearchParam(
            queryId = "1",
            matchPhrasePrefixQueryParams = listOf(QueryParam("entity.name", "gow", QueryParam.Operator.EQ, false)),
            queryParams = listOf(QueryParam("entity.education", "BE", )),
            orQueryParams = emptyList(),
            rangeParams = RangeParam(),
            uniqueParam = null,
            sortParams = emptyList(),
            from = 0,
            size = 10
        )
    )

    val results = esDao.query(searchList)
    println("query completed")

    results.forEach { t, u: ElasticSearchResult ->
        run {
            println(t)
            u.resultAsListMap.forEach { println(it) }
        }
    }
    println("query results print completed")

    println("<top>.main")

}
