package com.gvnavin.test

import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.opensearch.client.RestClientBuilder

class HttpClientConfigCallbackImpl: RestClientBuilder.HttpClientConfigCallback {
    override fun customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder {
        println("HttpClientConfigCallbackImpl.customizeHttpClient")

        httpClientBuilder.setSSLHostnameVerifier { _, _ -> true }
        return httpClientBuilder
    }
}