// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator

import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message

class EDPAggregatorUtils {
  companion object {
    /**
     * Converts a protobuf Message to a new DynamicMessage with a different structure,
     * using a single fieldValueMap for both field mapping and value conversion.
     *
     * @param originalMessage The source protobuf Message to convert from
     * @param newMessageDescriptor The descriptor of the target message type to convert to
     * @param fieldValueMap A mapping from original field paths with values to target field paths with values.
     *                      Keys are in the format "field1.field2.field3.value1"
     *                      Values are in the format "fielda.fieldb.value2"
     * @return A new DynamicMessage of the type specified by newMessageDescriptor with values converted from originalMessage
     */
    fun convertMessage(
      originalMessage: Message,
      newMessageDescriptor: Descriptor,
      fieldValueMap: Map<String, String>
    ): DynamicMessage {
      // Use a map to store builders for different nested message paths
      val builderMap = mutableMapOf<String, DynamicMessage.Builder>()

      // Create the root builder
      val rootBuilder = DynamicMessage.newBuilder(newMessageDescriptor)
      builderMap[""] = rootBuilder

      // First, gather all the field paths from the original message that we need to check
      val originalFieldPaths = fieldValueMap.keys.map {
        it.substring(0, it.lastIndexOf('.'))
      }.distinct()

      // Process each original field path to extract values
      for (originalFieldPath in originalFieldPaths) {
        val originalFieldValue = getFieldValueByPath(originalMessage, originalFieldPath)
        if (originalFieldValue != null) {
          val originalValueString = originalFieldValue.toString()
          val mappingKey = "$originalFieldPath.$originalValueString"

          // Check if this value has a mapping
          val targetPathWithValue = fieldValueMap[mappingKey]
          if (targetPathWithValue != null) {
            // Split target path into path and value
            val lastDotIndex = targetPathWithValue.lastIndexOf('.')
            if (lastDotIndex <= 0) continue // Invalid mapping

            val targetPath = targetPathWithValue.substring(0, lastDotIndex)
            val targetValue = targetPathWithValue.substring(lastDotIndex + 1)

            // Process the target path
            val pathParts = targetPath.split(".")

            // Track our progress through the path
            var currentPath = ""
            var currentBuilder = rootBuilder

            // Create all necessary nested message builders
            for (i in 0 until pathParts.size - 1) {
              val fieldName = pathParts[i]
              val nextPath = if (currentPath.isEmpty()) fieldName else "$currentPath.$fieldName"

              // Get or create builder for this path
              if (!builderMap.containsKey(nextPath)) {
                val fieldDescriptor = currentBuilder.descriptorForType.findFieldByName(fieldName)
                if (fieldDescriptor != null && fieldDescriptor.type == FieldDescriptor.Type.MESSAGE) {
                  val nestedBuilder = DynamicMessage.newBuilder(fieldDescriptor.messageType)
                  builderMap[nextPath] = nestedBuilder

                  // If the field is not set yet, set it with a new empty message
                  if (!currentBuilder.hasField(fieldDescriptor)) {
                    currentBuilder.setField(fieldDescriptor, nestedBuilder.build())
                  }
                } else {
                  break // Can't continue if field doesn't exist or isn't a message
                }
              }

              // Update current path and builder for next iteration
              currentPath = nextPath
              currentBuilder = builderMap[currentPath]!!

              // Re-get the message from the parent builder to ensure we're working with the latest version
              if (i > 0) {
                val parentPath = currentPath.substring(0, currentPath.lastIndexOf('.'))
                val parentBuilder = builderMap[parentPath]!!
                val parentFieldDescriptor = parentBuilder.descriptorForType.findFieldByName(fieldName)
                if (parentFieldDescriptor != null) {
                  // Rebuild the current builder if it's already set in parent
                  if (parentBuilder.hasField(parentFieldDescriptor)) {
                    val existingMessage = parentBuilder.getField(parentFieldDescriptor) as Message
                    currentBuilder = DynamicMessage.newBuilder(parentFieldDescriptor.messageType)
                    currentBuilder.mergeFrom(existingMessage)
                    builderMap[currentPath] = currentBuilder
                  }
                }
              }
            }

            // Set the actual field value on the innermost builder
            val finalFieldName = pathParts.last()
            val finalFieldDescriptor = currentBuilder.descriptorForType.findFieldByName(finalFieldName)

            if (finalFieldDescriptor != null) {
              val convertedValue = convertValueSimple(targetValue, finalFieldDescriptor)
              currentBuilder.setField(finalFieldDescriptor, convertedValue)

              // Update all parent builders with the new nested message values
              for (i in pathParts.size - 2 downTo 0) {
                val childPath = pathParts.subList(0, i + 1).joinToString(".")
                val parentPath = if (i > 0) pathParts.subList(0, i).joinToString(".") else ""

                val childBuilder = builderMap[childPath]!!
                val parentBuilder = builderMap[parentPath]!!

                val childFieldDescriptor = parentBuilder.descriptorForType.findFieldByName(pathParts[i])
                if (childFieldDescriptor != null) {
                  parentBuilder.setField(childFieldDescriptor, childBuilder.build())
                }
              }
            }
          }
        }
      }

      return rootBuilder.build()
    }

    /**
     * Retrieves a field value from a protobuf Message by following a dot-separated path.
     *
     * @param message The protobuf Message to extract the value from
     * @param fieldPath A dot-separated string representing the path to the field (e.g., "field1.field2.field3")
     * @return The value of the field at the specified path, or null if the path is invalid
     */
    private fun getFieldValueByPath(message: Message, fieldPath: String): Any? {
      val fieldNames = fieldPath.split(".")
      var currentMessage: Message = message
      for (fieldName in fieldNames.dropLast(1)) {
        val fieldDescriptor = currentMessage.descriptorForType.findFieldByName(fieldName)
        if (fieldDescriptor != null && fieldDescriptor.type == FieldDescriptor.Type.MESSAGE) {
          currentMessage = currentMessage.getField(fieldDescriptor) as Message
        } else {
          return null
        }
      }
      val finalFieldDescriptor = currentMessage.descriptorForType.findFieldByName(fieldNames.last())
      return finalFieldDescriptor?.let { currentMessage.getField(it) }
    }

    /**
     * Converts a string value to the appropriate type for the given field descriptor.
     *
     * @param valueString The string representation of the value to convert
     * @param fieldDescriptor The field descriptor indicating the target type
     * @return The converted value appropriate for the target field type
     */
    private fun convertValueSimple(
      valueString: String,
      fieldDescriptor: FieldDescriptor
    ): Any {
      return when (fieldDescriptor.type) {
        FieldDescriptor.Type.INT32 -> valueString.toInt()
        FieldDescriptor.Type.INT64 -> valueString.toLong()
        FieldDescriptor.Type.FLOAT -> valueString.toFloat()
        FieldDescriptor.Type.DOUBLE -> valueString.toDouble()
        FieldDescriptor.Type.BOOL -> valueString.toBoolean()
        FieldDescriptor.Type.STRING -> valueString
        FieldDescriptor.Type.ENUM -> fieldDescriptor.enumType.findValueByName(valueString)
        else -> valueString
      }
    }
  }
}

