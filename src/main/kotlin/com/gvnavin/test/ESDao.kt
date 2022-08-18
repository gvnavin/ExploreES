package com.gvnavin.test

import com.gvnavin.test.esreader.ElasticSearchParam
import com.gvnavin.test.esreader.ElasticSearchResult
import com.gvnavin.test.esreader.QueryParam
import com.gvnavin.test.eswriter.PublishType
import io.github.acm19.aws.interceptor.http.AwsRequestSigningApacheInterceptor
import org.apache.http.HttpHost
import org.apache.http.HttpRequestInterceptor
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.logging.log4j.LogManager
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.MultiSearchRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.update.UpdateRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.RestClient
import org.opensearch.client.RestClientBuilder
import org.opensearch.client.RestHighLevelClient
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
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.signer.Aws4Signer
import software.amazon.awssdk.regions.Region


class ESDao constructor(private val indexPrefix: String) {

    private val esClient = getEsClient(
        "https://localhost:9200",
        Region.AP_SOUTH_1.toString()
    )

    fun getEsClient(esEndPoint: String, signingRegion: String): RestHighLevelClient {

//      keytool -importcert -file node-0.example.com -alias certalias -keystore trusttest
        System.setProperty("javax.net.ssl.trustStore", "/opensearch/trusttest");
        System.setProperty("javax.net.ssl.trustStorePassword", "abc");

        //Only for demo purposes. Don't specify your credentials in code.
        //Only for demo purposes. Don't specify your credentials in code.
        val credentialsProvider: CredentialsProvider = BasicCredentialsProvider()
        credentialsProvider.setCredentials(
            AuthScope.ANY,
            UsernamePasswordCredentials("", "")
        )



        val interceptor: HttpRequestInterceptor = AwsRequestSigningApacheInterceptor(
            "es",
            Aws4Signer.create(),
            DefaultCredentialsProvider.create(),
            signingRegion
        )

        println("ESDao.getEsClient")

        return RestHighLevelClient(
            RestClient
                .builder(HttpHost.create(esEndPoint))
//                .setRequestConfigCallback { rcb ->
//                    rcb
//                        .setConnectTimeout(5 * 1000)
//                        .setSocketTimeout(120 * 1000)
//                }
                .setHttpClientConfigCallback { httpClientBuilder ->
                    httpClientBuilder.setDefaultCredentialsProvider(
                        credentialsProvider
                    )
                }
//                .setHttpClientConfigCallback{
//                    hacb ->
//                    hacb
//                        .addInterceptorLast(interceptor)
//                        .setMaxConnPerRoute(500)
//                        .setMaxConnTotal(500)
//                        .disableConnectionState()
//                    hacb.setSSLHostnameVerifier {  _, _ ->  true  }
//                }
        )
    }


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
        val resolvedIndexName = "people"
        val searchRequest = MultiSearchRequest()
        searchList.forEach { searchParam ->
            val boolQueryBuilder = QueryBuilders.boolQuery().apply {
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
                searchParam.rangeParams.rangeParams.forEach { rp ->
                    val rangeQuery = RangeQueryBuilder(rp.key)
                    val rangeFn = when (rp.operator) {
                        QueryParam.Operator.GTE -> rangeQuery::gte
                        QueryParam.Operator.GT -> rangeQuery::gt
                        QueryParam.Operator.LT -> rangeQuery::lt
                        QueryParam.Operator.LTE -> rangeQuery::lte
                        else -> throw NoWhenBranchMatchedException("Range params operation not supported: ${rp.operator}")
                    }
                    this.must(rangeFn(rp.value))
                }
            }
            val searchSourceBuilder = SearchSourceBuilder()
                .from(searchParam.from)
                .size(searchParam.size)
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
            searchRequest.add(SearchRequest(arrayOf(resolvedIndexName), searchSourceBuilder))
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