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

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.DescriptorProtos
import java.nio.file.Files
import java.util.Base64
import kotlin.test.assertFailsWith
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class ValidateSpannerProtoBundlesTest {
  @get:Rule val temporaryFolder = TemporaryFolder()

  @Test
  fun `main rejects nested enum without its parent`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        run("CREATE PROTO BUNDLE (`example.Parent.Mode`);", parent())
      }
    assertThat(error.message).contains("references unregistered type example.Parent")
    assertThat(error.message).contains("migration.sql::1::test")
  }

  @Test
  fun `main accepts nested enum with its parent`() {
    run("CREATE PROTO BUNDLE (`example.Parent`, `example.Parent.Mode`,);", parent())
  }

  @Test
  fun `main rejects missing message field dependency`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        run("CREATE PROTO BUNDLE (`example.A`);", message("A", "example.B"), message("B", null))
      }
    assertThat(error.message).contains("unregistered type example.B")
  }

  @Test
  fun `main accepts cyclic message dependencies`() {
    run(
      "CREATE PROTO BUNDLE (`example.A`, `example.B`);",
      message("A", "example.B"),
      message("B", "example.A"),
    )
  }

  @Test
  fun `main rejects deletion of a referenced type`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        run(
          "CREATE PROTO BUNDLE (`example.A`, `example.B`); " +
            "ALTER PROTO BUNDLE DELETE (`example.B`);",
          message("A", "example.B"),
          message("B", null),
        )
      }
    assertThat(error.message).contains("unregistered type example.B")
  }

  @Test
  fun `main accepts deletion of an entire dependent set`() {
    run(
      "CREATE PROTO BUNDLE (`example.Parent`, `example.Parent.Mode`); " +
        "ALTER PROTO BUNDLE DELETE (`example.Parent`, `example.Parent.Mode`);",
      parent(),
    )
  }

  @Test
  fun `main rejects unknown descriptors`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        run("CREATE PROTO BUNDLE (`example.Missing`);", message("A", null))
      }
    assertThat(error.message).contains("Missing descriptor for example.Missing")
  }

  @Test
  fun `main rejects unsupported bundle syntax`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        run("ALTER PROTO BUNDLE INSERT (`example.A`) unexpected;", message("A", null))
      }
    assertThat(error.message).contains("Unsupported ALTER PROTO BUNDLE")
  }

  @Test
  fun `main validates update against newly supplied descriptors`() {
    val root = temporaryFolder.newFolder().toPath()
    Files.writeString(
      root.resolve("first.sql"),
      migration("CREATE PROTO BUNDLE (`example.A`);", "1", message("A", null)),
    )
    Files.writeString(
      root.resolve("second.sql"),
      migration(
        "ALTER PROTO BUNDLE UPDATE (`example.A`);",
        "2",
        message("A", "example.B"),
        message("B", null),
      ),
    )
    val changelog = root.resolve("changelog.yaml")
    Files.writeString(
      changelog,
      """
      databaseChangeLog:
        - include:
            file: first.sql
            relativeToChangeLogFile: true
        - include:
            file: second.sql
            relativeToChangeLogFile: true
    """
        .trimIndent(),
    )
    val error =
      assertFailsWith<IllegalArgumentException> {
        ValidateSpannerProtoBundles.main(arrayOf(changelog.toString()))
      }
    assertThat(error.message).contains("second.sql::2::test")
    assertThat(error.message).contains("unregistered type example.B")
  }

  @Test
  fun `main checks each changelog independently`() {
    val root = temporaryFolder.newFolder().toPath()
    Files.writeString(
      root.resolve("first.sql"),
      migration("CREATE PROTO BUNDLE (`example.A`);", "1", message("A", null)),
    )
    Files.writeString(
      root.resolve("second.sql"),
      migration("CREATE PROTO BUNDLE (`example.A`);", "2", message("A", null)),
    )
    ValidateSpannerProtoBundles.main(
      arrayOf(root.resolve("first.sql").toString(), root.resolve("second.sql").toString())
    )
  }

  private fun run(sql: String, vararg messages: DescriptorProtos.DescriptorProto) {
    val path = temporaryFolder.newFolder().toPath().resolve("migration.sql")
    Files.writeString(path, migration(sql, "1", *messages))
    ValidateSpannerProtoBundles.main(arrayOf(path.toString()))
  }

  private fun migration(
    sql: String,
    id: String,
    vararg messages: DescriptorProtos.DescriptorProto,
  ): String {
    val descriptor =
      DescriptorProtos.FileDescriptorSet.newBuilder()
        .addFile(
          DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("example.proto")
            .setPackage("example")
            .setSyntax("proto3")
            .addAllMessageType(messages.toList())
        )
        .build()
    return "--liquibase formatted sql\n--changeset test:$id dbms:cloudspanner\n" +
      "SET PROTO_DESCRIPTORS = '${Base64.getEncoder().encodeToString(descriptor.toByteArray())}';\n$sql"
  }

  private fun message(name: String, dependency: String?): DescriptorProtos.DescriptorProto {
    return DescriptorProtos.DescriptorProto.newBuilder()
      .apply {
        setName(name)
        if (dependency != null) {
          addField(
            DescriptorProtos.FieldDescriptorProto.newBuilder()
              .setName("value")
              .setNumber(1)
              .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
              .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
              .setTypeName(".$dependency")
          )
        }
      }
      .build()
  }

  private fun parent(): DescriptorProtos.DescriptorProto {
    return DescriptorProtos.DescriptorProto.newBuilder()
      .setName("Parent")
      .addEnumType(
        DescriptorProtos.EnumDescriptorProto.newBuilder()
          .setName("Mode")
          .addValue(
            DescriptorProtos.EnumValueDescriptorProto.newBuilder()
              .setName("UNSPECIFIED")
              .setNumber(0)
          )
      )
      .build()
  }
}
