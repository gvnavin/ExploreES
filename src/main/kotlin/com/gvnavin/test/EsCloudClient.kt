package com.gvnavin.test

import io.github.acm19.aws.interceptor.http.AwsRequestSigningApacheInterceptor
import org.apache.http.HttpHost
import org.apache.http.HttpRequestInterceptor
import org.opensearch.client.RestClient
import org.opensearch.client.RestHighLevelClient
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.signer.Aws4Signer
import software.amazon.awssdk.regions.Region

val esClientForCloud = getEsClientForCloud(
    "",
    Region.AP_SOUTH_1.toString()
)

private fun getEsClientForCloud(esEndPoint: String, signingRegion: String): RestHighLevelClient {
    val interceptor: HttpRequestInterceptor = AwsRequestSigningApacheInterceptor(
        "es",
        Aws4Signer.create(),
        DefaultCredentialsProvider.create(),
        signingRegion
    )
    return RestHighLevelClient(
        RestClient
            .builder(HttpHost.create(esEndPoint))
            .setRequestConfigCallback { rcb ->
                rcb
                    .setConnectTimeout(5 * 1000)
                    .setSocketTimeout(120 * 1000)
            }
            .setHttpClientConfigCallback { hacb ->
                hacb
                    .addInterceptorLast(interceptor)
                    .setMaxConnPerRoute(500)
                    .setMaxConnTotal(500)
                    .disableConnectionState()
            }
    )
}
