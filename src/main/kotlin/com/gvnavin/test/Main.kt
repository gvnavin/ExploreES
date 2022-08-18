package com.gvnavin.test

import com.gvnavin.test.esreader.ElasticSearchParam
import com.gvnavin.test.esreader.QueryParam
import com.gvnavin.test.esreader.RangeParam
import com.gvnavin.test.eswriter.PublishType


fun main(args: Array<String>) {
    println("Hello")

    val jsonObject = """{
        "age": 10,
        "dateOfBirth": 1471466076564,
        "fullName": "John Doe"
        }"""

    val esDao = ESDao("")
    esDao.storeIndex("people", jsonObject, "1", PublishType.INSERT)
    println("store index completed")

    val searchList: ArrayList<ElasticSearchParam> = ArrayList()

    searchList.add(
        ElasticSearchParam(
            queryId = "1",
            queryParams = listOf(QueryParam("fullName", "John Doe", )),
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

    results.forEach { t, u ->
        run {
            println(t)
            println(u)
        }
    }
    println("query results print completed")

    println("<top>.main")

}
