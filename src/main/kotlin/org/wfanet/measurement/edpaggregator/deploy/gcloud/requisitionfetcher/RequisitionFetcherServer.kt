/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.deploy.gcloud.requisitionfetcher

import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStream
import java.io.OutputStream
import java.io.OutputStreamWriter
import java.io.PrintWriter
import java.net.InetSocketAddress
import java.net.URI
import java.util.Optional
import java.util.logging.Logger

/**
 * HTTP server wrapper for RequisitionFetcherFunction to run in Cloud Run containers.
 *
 * This server adapts the Cloud Functions HttpFunction interface to work with a standard
 * HTTP server, enabling deployment as a Cloud Run container instead of a Cloud Function Gen 2.
 */
fun main() {
  val port = System.getenv("PORT")?.toIntOrNull() ?: 8080
  val function = RequisitionFetcherFunction()
  val server = HttpServer.create(InetSocketAddress(port), 0)

  server.createContext("/") { exchange ->
    try {
      logger.info("Received ${exchange.requestMethod} request to ${exchange.requestURI}")

      val httpRequest = CloudRunHttpRequest(exchange)
      val httpResponse = CloudRunHttpResponse(exchange)

      function.service(httpRequest, httpResponse)

      // Flush and close the response
      httpResponse.flush()

      logger.info("Request completed with status ${exchange.responseCode}")
    } catch (e: Exception) {
      logger.severe("Error processing request: ${e.message}")
      e.printStackTrace()

      if (!exchange.responseHeaders.containsKey("Content-Type")) {
        exchange.responseHeaders.set("Content-Type", "text/plain")
      }

      val errorMessage = "Internal server error: ${e.message}"
      exchange.sendResponseHeaders(500, errorMessage.length.toLong())
      exchange.responseBody.use { it.write(errorMessage.toByteArray()) }
    }
  }

  server.start()
  logger.info("RequisitionFetcher HTTP server started on port $port")
}

private val logger: Logger = Logger.getLogger("RequisitionFetcherServer")

/**
 * Adapts HttpExchange to Cloud Functions HttpRequest interface.
 */
private class CloudRunHttpRequest(private val exchange: HttpExchange) : HttpRequest {

  override fun getMethod(): String = exchange.requestMethod

  override fun getUri(): String = exchange.requestURI.toString()

  override fun getPath(): String = exchange.requestURI.path

  override fun getQuery(): Optional<String> = Optional.ofNullable(exchange.requestURI.query)

  override fun getHeaders(): MutableMap<String, MutableList<String>> {
    val headers = mutableMapOf<String, MutableList<String>>()
    for ((key, values) in exchange.requestHeaders) {
      headers[key] = values.toMutableList()
    }
    return headers
  }

  override fun getFirstHeader(name: String): Optional<String> {
    return Optional.ofNullable(exchange.requestHeaders.getFirst(name))
  }

  override fun getContentType(): Optional<String> {
    return Optional.ofNullable(exchange.requestHeaders.getFirst("Content-Type"))
  }

  override fun getContentLength(): Long {
    val contentLength = exchange.requestHeaders.getFirst("Content-Length")
    return contentLength?.toLongOrNull() ?: -1L
  }

  override fun getCharacterEncoding(): Optional<String> {
    val contentType = exchange.requestHeaders.getFirst("Content-Type") ?: return Optional.empty()
    val charsetPrefix = "charset="
    val charsetIndex = contentType.indexOf(charsetPrefix)
    if (charsetIndex >= 0) {
      val charset = contentType.substring(charsetIndex + charsetPrefix.length).trim()
      return Optional.of(charset)
    }
    return Optional.empty()
  }

  override fun getInputStream(): InputStream = exchange.requestBody

  override fun getReader(): BufferedReader = exchange.requestBody.bufferedReader()

  override fun getQueryParameters(): MutableMap<String, MutableList<String>> {
    val params = mutableMapOf<String, MutableList<String>>()
    val query = exchange.requestURI.query ?: return params

    for (param in query.split("&")) {
      val parts = param.split("=", limit = 2)
      if (parts.isNotEmpty()) {
        val key = parts[0]
        val value = if (parts.size > 1) parts[1] else ""
        params.computeIfAbsent(key) { mutableListOf() }.add(value)
      }
    }

    return params
  }

  override fun getFirstQueryParameter(name: String): Optional<String> {
    return Optional.ofNullable(getQueryParameters()[name]?.firstOrNull())
  }

  override fun getParts(): MutableMap<String, HttpRequest.HttpPart> {
    // Multipart form data not needed for this use case
    return mutableMapOf()
  }
}

/**
 * Adapts HttpExchange to Cloud Functions HttpResponse interface.
 */
private class CloudRunHttpResponse(private val exchange: HttpExchange) : HttpResponse {
  private var statusCode: Int = 200
  private var headersSent: Boolean = false
  private val responseBuffer = StringBuilder()
  private var outputStreamUsed = false

  override fun setStatusCode(code: Int) {
    statusCode = code
  }

  override fun setStatusCode(code: Int, message: String?) {
    statusCode = code
  }

  override fun setContentType(contentType: String) {
    exchange.responseHeaders.set("Content-Type", contentType)
  }

  override fun getContentType(): Optional<String> {
    return Optional.ofNullable(exchange.responseHeaders.getFirst("Content-Type"))
  }

  override fun appendHeader(header: String, value: String) {
    exchange.responseHeaders.add(header, value)
  }

  override fun getHeaders(): MutableMap<String, MutableList<String>> {
    val headers = mutableMapOf<String, MutableList<String>>()
    for ((key, values) in exchange.responseHeaders) {
      headers[key] = values.toMutableList()
    }
    return headers
  }

  override fun getOutputStream(): OutputStream {
    outputStreamUsed = true
    return ResponseOutputStream()
  }

  override fun getWriter(): BufferedWriter {
    return object : BufferedWriter(OutputStreamWriter(ResponseOutputStream())) {
      override fun write(str: String) {
        responseBuffer.append(str)
      }

      override fun write(cbuf: CharArray, off: Int, len: Int) {
        responseBuffer.append(cbuf, off, len)
      }

      override fun write(c: Int) {
        responseBuffer.append(c.toChar())
      }
    }
  }

  fun flush() {
    if (!headersSent && !outputStreamUsed) {
      val responseBytes = responseBuffer.toString().toByteArray()
      exchange.sendResponseHeaders(statusCode, responseBytes.size.toLong())
      exchange.responseBody.use { it.write(responseBytes) }
      headersSent = true
    }
  }

  private inner class ResponseOutputStream : OutputStream() {
    private var headersSentLocal = false

    override fun write(b: Int) {
      ensureHeadersSent()
      exchange.responseBody.write(b)
    }

    override fun write(b: ByteArray) {
      ensureHeadersSent()
      exchange.responseBody.write(b)
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
      ensureHeadersSent()
      exchange.responseBody.write(b, off, len)
    }

    override fun flush() {
      ensureHeadersSent()
      exchange.responseBody.flush()
    }

    override fun close() {
      ensureHeadersSent()
      exchange.responseBody.close()
    }

    private fun ensureHeadersSent() {
      if (!headersSentLocal) {
        exchange.sendResponseHeaders(statusCode, 0) // 0 for chunked transfer
        headersSentLocal = true
        headersSent = true
      }
    }
  }
}
