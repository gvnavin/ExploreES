package com.gvnavin.test

import com.gvnavin.test.esreader.ElasticSearchParam
import com.gvnavin.test.esreader.ElasticSearchResult
import com.gvnavin.test.esreader.QueryParam
import com.gvnavin.test.eswriter.PublishType
import org.apache.logging.log4j.LogManager
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.MultiSearchRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.update.UpdateRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.query.RangeQueryBuilder
import org.opensearch.script.Script
import org.opensearch.script.ScriptType
import org.opensearch.search.aggregations.AggregationBuilders
import org.opensearch.search.aggregations.metrics.ParsedCardinality
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.collapse.CollapseBuilder
import org.opensearch.search.sort.ScriptSortBuilder
import org.opensearch.search.sort.SortBuilders
import org.opensearch.search.sort.SortOrder
import software.amazon.awssdk.regions.Region


class ESDao constructor(private val indexPrefix: String) {

    private val esClient = esClientForCloud

    fun storeIndex(index: String, jsonString: String, uniqueId: String, type: PublishType) {
        when(type) {
            PublishType.INSERT -> IndexRequest("${indexPrefix}${index}").apply {
                id(uniqueId); source(jsonString, XContentType.JSON)
                esClient.index(this, RequestOptions.DEFAULT)
            }
            PublishType.UPDATE -> UpdateRequest("${indexPrefix}${index}", uniqueId).apply {
                doc(jsonString, XContentType.JSON)
                esClient.update(this, RequestOptions.DEFAULT)
            }
        }
    }

    fun deleteIndex(index: String, uniqueId: String) = DeleteRequest("${indexPrefix}${index}", uniqueId).apply {
        esClient.delete(this, RequestOptions.DEFAULT)
    }

    fun createIndexIfNotExists(indexName: String): String {
        val exists = GetIndexRequest(indexName).run { esClient.indices().exists(this, RequestOptions.DEFAULT) }
        return if (!exists) {
            CreateIndexRequest(indexName)
                .run { esClient.indices().create(this, RequestOptions.DEFAULT) }
                .index()
        } else {
            "Exists"
        }
    }

    fun query(searchList: List<ElasticSearchParam>): Map<Any, ElasticSearchResult> {
        val searchRequest = MultiSearchRequest()
        searchList.forEach { searchParam ->

            val boolQueryBuilder = QueryBuilders.boolQuery().apply {

                searchParam.matchPhrasePrefixQueryParams.forEach { qp ->
                    this.must(QueryBuilders.matchPhrasePrefixQuery(getKey(qp), qp.value))
                }
                searchParam.queryParams.forEach { qp ->
                    when (qp.operator) {
                        QueryParam.Operator.IN -> this.must(QueryBuilders.termsQuery(getKey(qp), qp.value as Collection<*>))
                        QueryParam.Operator.NOT_IN -> this.mustNot(QueryBuilders.termsQuery(getKey(qp), qp.value as Collection<*>))
                        QueryParam.Operator.EQ -> this.must(QueryBuilders.termQuery(getKey(qp), qp.value))
                        else -> throw NoWhenBranchMatchedException("Search params operation not supported: ${qp.operator}")
                    }
                }
                searchParam.orQueryParams.forEach { qp ->
                    this.must(getShouldBoolQuery(qp))
                }
//                searchParam.rangeParams.rangeParams.forEach { rp ->
//                    val rangeQuery = RangeQueryBuilder(rp.key)
//                    val rangeFn = when (rp.operator) {
//                        QueryParam.Operator.GTE -> rangeQuery::gte
//                        QueryParam.Operator.GT -> rangeQuery::gt
//                        QueryParam.Operator.LT -> rangeQuery::lt
//                        QueryParam.Operator.LTE -> rangeQuery::lte
//                        else -> throw NoWhenBranchMatchedException("Range params operation not supported: ${rp.operator}")
//                    }
//                    this.must(rangeFn(rp.value))
//                }
            }
            val searchSourceBuilder = SearchSourceBuilder()
//                .from(searchParam.from)
//                .size(searchParam.size)
                .version(true)
                .timeout(TimeValue.timeValueMillis(ES_QUERY_TIMEOUT))
                .query(boolQueryBuilder)

            searchParam.sortParams.forEach {
                val sortBuilder = if (!it.script) {
                    SortBuilders
                        .fieldSort(it.key)
                } else {
                    val script = Script(
                        ScriptType.INLINE,
                        "painless",
                        it.key,
                        it.params
                    )
                    SortBuilders
                        .scriptSort(script, ScriptSortBuilder.ScriptSortType.NUMBER)
                }
                searchSourceBuilder.sort(sortBuilder.order(SortOrder.fromString(it.order.name)))
            }

            if (searchParam.uniqueParam != null) {
                val cardinalityAgg = AggregationBuilders
                    .cardinality("total")
                    .field("${searchParam.uniqueParam}.keyword")

                searchSourceBuilder
                    .collapse(CollapseBuilder("${searchParam.uniqueParam}.keyword"))
                    .aggregation(cardinalityAgg)
            }
            searchRequest.add(SearchRequest(arrayOf(indexPrefix), searchSourceBuilder))
        }

        println("ESDao.query println(searchRequest)")
        println(searchRequest)



        val result = esClient.msearch(searchRequest, RequestOptions.DEFAULT)
        return result.responses.mapIndexedNotNull { respIndex, resp ->
            if (resp.isFailure) {
                LOGGER.error("error while querying: ${resp.failureMessage}", resp.failure)
                null
            } else {
                val total = resp?.response?.aggregations
                    ?.get<ParsedCardinality>("total")?.value
                    ?: resp?.response?.hits?.totalHits?.value
                    ?: 0L
                Pair(
                    searchList[respIndex].queryId,
                    ElasticSearchResult(
                        total = total,
                        resultAsListMap = resp.response.hits.map { it.sourceAsMap },
                        resultAsListString = resp.response.hits.map { it.sourceAsString },
                        size = resp.response.hits.hits.size,
                        start = searchList[respIndex].from
                    )
                )
            }
        }.toMap()
    }

    private fun getShouldBoolQuery(queries: List<QueryParam>) : BoolQueryBuilder? {
        return QueryBuilders.boolQuery().apply {
            queries.forEach { qp ->
                when (qp.operator) {
                    QueryParam.Operator.IN -> this.should(QueryBuilders.termsQuery(getKey(qp), qp.value as Collection<*>))
                    QueryParam.Operator.NOT_IN -> this.mustNot(QueryBuilders.termsQuery(getKey(qp), qp.value as Collection<*>))
                    QueryParam.Operator.EQ -> this.should(QueryBuilders.termQuery(getKey(qp), qp.value))
                    else -> throw NoWhenBranchMatchedException("Search params operation not supported: ${qp.operator}")
                }
            }
        }
    }

    private fun getKey(qp: QueryParam): String {
        return if (qp.isKeyword) {
            "${qp.key}.keyword"
        } else {
            qp.key
        }
    }

    companion object {
        private const val ES_QUERY_TIMEOUT = 1000L // 1 second timeout for each query
        private val LOGGER = LogManager.getLogger(ESDao::class.java)
    }
}