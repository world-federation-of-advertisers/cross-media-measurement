// Copyright 2020 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.tools

import com.google.gson.JsonObject
import com.google.gson.JsonParser.parseReader
import com.google.gson.stream.JsonReader
import java.io.Reader
import java.lang.Exception
import java.util.stream.Collectors
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

/**
 * Fetch state of a Kubernetes-in-Docker (kind) cluster.
 *
 * @param clusterName Name passed to `kind create cluster`.
 */
class ClusterState(private val clusterName: String = "kind") {
  init {
    require(Regex("[A-z0-9_-]+").matches(clusterName))
  }

  /**
   *
   */
  fun getNodeIp(): String =
    handleJson("kubectl get node $clusterName-control-plane -o json") { json ->
      json
        .getAsJsonObject("status")
        .getAsJsonArray("addresses")
        .first { el ->
          el.asJsonObject.getAsJsonPrimitive("type").asString == "InternalIP"
        }.asJsonObject.getAsJsonPrimitive("address").asString
    }

  /**
   * Parses Kubernetes Service specs to find the ones that have node ports and return them.
   *
   * For example the following JSON would return the same Map as the Kotlin code:
   * mapOf(Pair("my-service", mapOf(Pair("grpc", 30001), Pair("http", 30002))))
   *
   * {
   *   "items": [
   *     "kind": Service
   *     "metadata": {
   *       "name": "my-service"
   *     }
   *     "spec": {
   *       "type": "NodePort"
   *       "ports": [
   *         {
   *           "name": "grpc"
   *           "nodePort": 30001
   *         }
   *         {
   *           "name": "http"
   *           "nodePort": 30002
   *         }
   *       ]
   *     }
   *   ]
   * }
   *
   * @return Outer map contains one entry per NodePort type service keyed on the service name. The
   *     key of the inner map is the port name and the value is the port number.
   */
  fun getNodePorts(jobNames: List<String>): Map<String, Map<String, Int>> =
    handleJson(
      "kubectl get services ${
      jobNames.joinToString(" ")
      } -o json --context kind-$clusterName"
    ) { json ->
      json
        .getAsJsonArray("items")
        .map { it.asJsonObject }
        .filter { el ->
          el.getAsJsonPrimitive("kind").asString == "Service" &&
            el.getAsJsonObject("spec")
            .getAsJsonPrimitive("type").asString == "NodePort"
        }.associate { el ->
          Pair(
            el.getAsJsonObject("metadata")
              .getAsJsonPrimitive("name").asString,
            el.getAsJsonObject("spec")
              .getAsJsonArray("ports")
              .associate {
                Pair(
                  it.asJsonObject.getAsJsonPrimitive("name").asString,
                  it.asJsonObject.getAsJsonPrimitive("nodePort").asInt
                )
              }
          )
        }
    }

  fun jobsSucceeded(jobNames: List<String>): Boolean =
    handleJson(
      "kubectl get jobs ${
      jobNames.joinToString(" ")
      } -o json --context kind-$clusterName"
    ) { json ->
      json
        .getAsJsonArray("items")
        .all { el ->
          el.asJsonObject
            .getAsJsonObject("status").has("succeeded")
        }
    }

  fun getPodStatuses(): Map<String, String> =
    exec(
      listOf(
        "kubectl", "get", "pods", "--context", "kind-$clusterName",
        "-o=jsonpath={range .items[*]}{.metadata.name}{\"=\"}{.status.phase}{\"\\n\"}{end}"
      )
    ) { reader ->
      reader.readLines().associate {
        val str = it.split("=")
        require(str.size == 2)
        Pair(str[0], str[1])
      }
    }
}

private fun <T> handleJson(command: String, parse: (JsonObject) -> T): T =
  exec(command.split("\\s".toRegex())) { reader ->
    val element = parseReader(JsonReader(reader))
    parse(element.asJsonObject)
  }

/** Runs a subprocess and runs its stdout through the parser. */
private fun <T> exec(
  command: List<String>,
  parse: (Reader) -> T
): T {
  val process = ProcessBuilder(command).start()
  var errorMessage: String? = null
  var result: T? = null

  runBlocking {
    joinAll(
      launch {
        errorMessage = process.errorStream.bufferedReader().lines()
          .collect(Collectors.joining(System.lineSeparator()))
      },
      launch {
        result = parse(process.inputStream.bufferedReader())
      }
    )
  }

  process.waitFor()

  // Note that this check is somewhat unreliable since some applications, kubectl in particular,
  // return exit value zero and write error messages to stdout when they receive invalid arguments.
  if (process.exitValue() != 0) {
    throw Exception(errorMessage)
  }

  return result!!
}
