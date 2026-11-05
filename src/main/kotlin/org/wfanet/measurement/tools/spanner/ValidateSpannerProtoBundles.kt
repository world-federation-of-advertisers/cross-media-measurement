// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.tools.spanner

import com.google.protobuf.DescriptorProtos
import java.nio.file.Path
import java.util.Base64
import liquibase.Scope
import liquibase.change.AbstractSQLChange
import liquibase.changelog.ChangeLogParameters
import liquibase.parser.ChangeLogParserFactory
import liquibase.resource.DirectoryResourceAccessor
import liquibase.util.StringUtil

/** Checks Spanner proto bundle membership in Liquibase migration order. */
object ValidateSpannerProtoBundles {
  private data class Definition(val dependencies: Set<String>)

  private val descriptorStatement = Regex("(?is)SET\\s+PROTO_DESCRIPTORS\\s*=\\s*'([^']*)'")
  private val createStatement = Regex("(?is)CREATE\\s+PROTO\\s+BUNDLE\\s*\\(([^)]*)\\)")
  private val alterStatement = Regex("(?is)ALTER\\s+PROTO\\s+BUNDLE\\s+(.+)")
  private val action = Regex("(?is)(INSERT|UPDATE|DELETE)\\s*\\(([^)]*)\\)")
  private val typeName = Regex("[A-Za-z_][A-Za-z_0-9]*(?:\\.[A-Za-z_][A-Za-z_0-9]*)*")

  @JvmStatic
  fun main(args: Array<String>) {
    require(args.isNotEmpty()) { "Usage: ValidateSpannerProtoBundles <changelog>..." }
    for (arg in args) {
      validate(Path.of(arg).toAbsolutePath().normalize())
    }
  }

  private fun validate(path: Path) {
    DirectoryResourceAccessor(path.fileSystem.rootDirectories.first()).use { accessor ->
      Scope.child(mapOf(Scope.Attr.resourceAccessor.name to accessor)) {
        val changelog =
          ChangeLogParserFactory.getInstance()
            .getParser(path.toString(), accessor)
            .parse(path.toString(), ChangeLogParameters(), accessor)
        val definitions = mutableMapOf<String, Definition>()
        val bundle = mutableMapOf<String, Definition>()
        for (changeSet in changelog.changeSets) {
          for (change in changeSet.changes) {
            if (change !is AbstractSQLChange) continue
            val sql = change.sql ?: continue
            for (statement in StringUtil.processMultiLineSQL(sql, true, true, ";")) {
              try {
                apply(statement.trim(), definitions, bundle)
              } catch (e: IllegalArgumentException) {
                throw IllegalArgumentException(
                  "${changeSet.filePath}::${changeSet.id}::${changeSet.author}: ${e.message}",
                  e,
                )
              }
            }
          }
        }
      }
    }
  }

  private fun apply(
    sql: String,
    definitions: MutableMap<String, Definition>,
    bundle: MutableMap<String, Definition>,
  ) {
    descriptorStatement.matchEntire(sql)?.let { match ->
      val descriptors =
        DescriptorProtos.FileDescriptorSet.parseFrom(
          Base64.getDecoder().decode(match.groupValues[1])
        )
      definitions.clear()
      for (file in descriptors.fileList) {
        for (message in file.messageTypeList) {
          addMessage(message, file.`package`, null, definitions)
        }
        for (enum in file.enumTypeList) {
          definitions["${file.`package`}.${enum.name}"] = Definition(emptySet())
        }
      }
      return
    }

    val next = bundle.toMutableMap()
    val create = createStatement.matchEntire(sql)
    val alter = alterStatement.matchEntire(sql)
    if (create != null) {
      require(bundle.isEmpty()) { "PROTO BUNDLE already exists" }
      insert(names(create.groupValues[1]), definitions, next)
    } else if (alter != null) {
      val changes = action.findAll(alter.groupValues[1]).toList()
      require(changes.isNotEmpty() && action.replace(alter.groupValues[1], "").isBlank()) {
        "Unsupported ALTER PROTO BUNDLE statement"
      }
      require(changes.map { it.groupValues[1].uppercase() }.distinct().size == changes.size) {
        "Repeated PROTO BUNDLE action"
      }
      for (match in changes) {
        val types = names(match.groupValues[2])
        when (match.groupValues[1].uppercase()) {
          "INSERT" -> insert(types, definitions, next)
          "UPDATE" ->
            for (name in types) {
              require(name in next) { "Cannot update unregistered type $name" }
              next[name] = requireNotNull(definitions[name]) { "Missing descriptor for $name" }
            }
          "DELETE" ->
            for (name in types) {
              require(next.remove(name) != null) { "Cannot delete unregistered type $name" }
            }
        }
      }
    } else {
      require(!Regex("(?is)^(CREATE|ALTER)\\s+PROTO\\s+BUNDLE\\b").containsMatchIn(sql)) {
        "Unsupported PROTO BUNDLE statement"
      }
      return
    }

    for ((name, definition) in next) {
      for (dependency in definition.dependencies) {
        require(dependency in next) { "$name references unregistered type $dependency" }
      }
    }
    bundle.clear()
    bundle.putAll(next)
  }

  private fun names(text: String): List<String> {
    val names = text.trim().removeSuffix(",").split(',').map { it.trim().removeSurrounding("`") }
    require(names.all { typeName.matches(it) } && names.distinct().size == names.size) {
      "Invalid PROTO BUNDLE type list"
    }
    return names
  }

  private fun insert(
    names: List<String>,
    definitions: Map<String, Definition>,
    bundle: MutableMap<String, Definition>,
  ) {
    for (name in names) {
      require(name !in bundle) { "Type $name is already registered" }
      bundle[name] = requireNotNull(definitions[name]) { "Missing descriptor for $name" }
    }
  }

  private fun addMessage(
    message: DescriptorProtos.DescriptorProto,
    prefix: String,
    parent: String?,
    definitions: MutableMap<String, Definition>,
  ) {
    val name = "$prefix.${message.name}"
    definitions[name] =
      Definition(
        buildSet {
          if (parent != null) add(parent)
          for (field in message.fieldList) {
            if (field.typeName.isNotEmpty()) add(field.typeName.removePrefix("."))
          }
        }
      )
    for (enum in message.enumTypeList) {
      definitions["$name.${enum.name}"] = Definition(setOf(name))
    }
    for (nested in message.nestedTypeList) {
      addMessage(nested, name, name, definitions)
    }
  }
}
