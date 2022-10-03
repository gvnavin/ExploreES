package com.gvnavin.test

import io.github.acm19.aws.interceptor.http.AwsRequestSigningApacheInterceptor
import org.apache.http.HttpHost
import org.apache.http.HttpRequestInterceptor
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.opensearch.client.RestClient
import org.opensearch.client.RestHighLevelClient
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.signer.Aws4Signer
import software.amazon.awssdk.regions.Region


val esLocalClient = getEsLocalClient(
"https://localhost:9200",
    Region.AP_SOUTH_1.toString()
)

fun getEsLocalClient(esEndPoint: String, signingRegion: String): RestHighLevelClient {

//      keytool -importcert -file node-0.example.com -alias certalias -keystore trusttest
    System.setProperty("javax.net.ssl.trustStore", "/home/gvnavin/opensearch/trusttest");
    System.setProperty("javax.net.ssl.trustStorePassword", "123456");

    //Only for demo purposes. Don't specify your credentials in code.
    //Only for demo purposes. Don't specify your credentials in code.
    val credentialsProvider: CredentialsProvider = BasicCredentialsProvider()
    credentialsProvider.setCredentials(
        AuthScope.ANY,
        UsernamePasswordCredentials("admin", "admin")
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
