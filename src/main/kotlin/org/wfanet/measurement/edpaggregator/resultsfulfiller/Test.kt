package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.ExtensionRegistry

import com.google.protobuf.TypeRegistry
import java.io.File
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Paths
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto

import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.StorageParams


class Test {



  companion object {

    private val EXTENSION_REGISTRY =
      ExtensionRegistry.newInstance()
        .also { EventAnnotationsProto.registerAllExtensions(it) }
        .unmodifiable

    private fun loadFileDescriptorSets(
      files: Iterable<File>
    ): List<DescriptorProtos.FileDescriptorSet> {
      return files.map { file ->
        println("Input size: ${file.length()}")
        file.inputStream().use { input ->

        DescriptorProtos.FileDescriptorSet.parseFrom(input, EXTENSION_REGISTRY)
//          DescriptorProtos.FileDescriptorSet.parseFrom(input)
        }
      }
    }

    private fun buildTypeRegistry(): TypeRegistry {
      return TypeRegistry.newBuilder()
        .apply {
          add(COMPILED_PROTOBUF_TYPES.flatMap { it.messageTypes })
          if (::eventTemplateDescriptorSetFiles.isInitialized) {
            add(
              ProtoReflection.buildDescriptors(
                loadFileDescriptorSets(eventTemplateDescriptorSetFiles),
                COMPILED_PROTOBUF_TYPES,
              )
            )
          }
        }
        .build()
    }
    private val COMPILED_PROTOBUF_TYPES: Iterable<Descriptors.FileDescriptor> =
      (ProtoReflection.WELL_KNOWN_TYPES.asSequence() + ResultsFulfillerParams.getDescriptor().file)
        .asIterable()

    fun createEmptyFile(path: String): File {
      val file = File(path)
      // make sure parent directories exist
      file.parentFile?.let { it.mkdirs() }

      // createNewFile returns false if the file already existed
      if (file.createNewFile()) {
        println("Created empty file at: $path")
      } else {
        println("File already exists at: $path (emptied it)")
        // if you want to truncate an existing file:
        file.writeText("")
      }
      return file
    }

    private lateinit var eventTemplateDescriptorSetFiles: List<File>
    @JvmStatic
    fun main(args: Array<String>) {
      println("Hello from companion object")

      val path = getRuntimePath(
        Paths.get("wfa_measurement_system",
          "src", "main", "kotlin", "org", "wfanet", "measurement",
          "edpaggregator", "resultsfulfiller", "testing")
      )

      Files.list(path).use { stream ->
        stream.forEach { path ->
          println(path.fileName)      // just the name
        }
      }

      val descriptorFile = File(path!!.resolve("event_template_metadata_type.pb").toString())
    val file2 = createEmptyFile(path.resolve("test.pb").toString())


      eventTemplateDescriptorSetFiles = listOf(descriptorFile)
//      eventTemplateDescriptorSetFiles = listOf(file2)
      val typeRegistry = buildTypeRegistry()

      println("Loaded TypeRegistry with ")
    }
  }





}
