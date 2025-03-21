package org.wfanet.measurement.securecomputation.teeapps.utils

import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors.EnumValueDescriptor
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import com.google.protobuf.util.JsonFormat

class TeeApplicationUtils {
  companion object {
    fun convertMessage(
      originalMessage: Message,
      newMessageDescriptor: Descriptor,
      fieldNameMap: Map<String, String>,
      fieldValueMap: Map<String, String>,
    ): DynamicMessage {
      val builder = DynamicMessage.newBuilder(newMessageDescriptor)

      for (originalMessageFieldDescriptor in originalMessage.descriptorForType.fields) {
        val originalMessageFieldName = originalMessageFieldDescriptor.name
        val originalMessageFieldValueString = originalMessage.getField(originalMessageFieldDescriptor).toString()

        val newMessageFieldName = fieldNameMap[originalMessageFieldName]
        val newMessageFieldValueString = fieldValueMap[originalMessageFieldValueString]!!
        val newMessageFieldDescriptor = newMessageDescriptor.findFieldByName(newMessageFieldName)

        when (newMessageFieldDescriptor.type) {
          // Integer types
          FieldDescriptor.Type.INT32, FieldDescriptor.Type.SINT32, FieldDescriptor.Type.FIXED32, FieldDescriptor.Type.SFIXED32 ->
            builder.setField(newMessageFieldDescriptor, newMessageFieldValueString.toInt())

          // Long types
          FieldDescriptor.Type.INT64, FieldDescriptor.Type.SINT64, FieldDescriptor.Type.FIXED64, FieldDescriptor.Type.SFIXED64 ->
            builder.setField(newMessageFieldDescriptor, newMessageFieldValueString.toLong())

          // Float and Double
          FieldDescriptor.Type.FLOAT ->
            builder.setField(newMessageFieldDescriptor, newMessageFieldValueString.toFloat())

          FieldDescriptor.Type.DOUBLE ->
            builder.setField(newMessageFieldDescriptor, newMessageFieldValueString.toDouble())

          // Boolean
          FieldDescriptor.Type.BOOL ->
            builder.setField(newMessageFieldDescriptor, newMessageFieldValueString.lowercase() == "true")

          // String - direct use
          FieldDescriptor.Type.STRING ->
            builder.setField(newMessageFieldDescriptor, newMessageFieldValueString)

          // Bytes - convert string to ByteString
          FieldDescriptor.Type.BYTES ->
            builder.setField(newMessageFieldDescriptor, ByteString.copyFrom(newMessageFieldValueString.toByteArray()))

          // Enum types - find enum value by name
          FieldDescriptor.Type.ENUM -> {
            val enumType = newMessageFieldDescriptor.enumType
            val enumValue = enumType.findValueByName(newMessageFieldValueString)
              ?: throw IllegalArgumentException(
                "Invalid enum value '$newMessageFieldValueString' for enum type ${enumType.fullName}"
              )
            builder.setField(newMessageFieldDescriptor, enumValue)
          }

          // Message types - parse from JSON
          FieldDescriptor.Type.MESSAGE -> {
            try {
              // This assumes newMessageFieldValueString is a valid JSON representation of the message
              val jsonParser =JsonFormat.parser()
              val fieldMessageBuilder = DynamicMessage.newBuilder(newMessageFieldDescriptor.messageType)
              jsonParser.merge(newMessageFieldValueString, fieldMessageBuilder)
              builder.setField(newMessageFieldDescriptor, fieldMessageBuilder.build())
            } catch (e: Exception) {
              throw IllegalArgumentException(
                "Failed to parse message value '$newMessageFieldValueString' for message type ${newMessageFieldDescriptor.messageType.fullName}",
                e
              )
            }
          }

          // For other types, throw an exception
          else -> throw IllegalArgumentException(
            "Unsupported field type: ${newMessageFieldDescriptor.type} for field ${newMessageFieldDescriptor.fullName}"
          )
        }
      }
      return builder.build()
    }

    /**
     * Converts a value to the appropriate type for the target field.
     */
    private fun convertValueToTargetType(value: Any, fieldDescriptor: FieldDescriptor): Any? {
      return when (fieldDescriptor.javaType) {
        FieldDescriptor.JavaType.MESSAGE -> {
          when (value) {
            // If value is already a Message, check if it's compatible
            is Message -> {
              if (value.descriptorForType == fieldDescriptor.messageType) {
                // Already the correct type
                value
              } else {
                // Would need custom conversion between different message types
                null
              }
            }

            else -> null
          }
        }

        FieldDescriptor.JavaType.ENUM -> {
          when (value) {
            // If value is already an EnumValueDescriptor, check if it belongs to the right enum type
            is EnumValueDescriptor -> {
              if (value.type == fieldDescriptor.enumType) value else null
            }
            // If value is a Java/Kotlin enum
            is Enum<*> -> fieldDescriptor.enumType.findValueByName(value.name)
            // If value is a string, try to find corresponding enum value
            is String -> fieldDescriptor.enumType.findValueByName(value)
            // If value is an integer, try to find corresponding enum value by number
            is Int -> fieldDescriptor.enumType.findValueByNumber(value)
            // If nothing else works, try toString and find by name
            else -> fieldDescriptor.enumType.findValueByName(value.toString())
          }
        }

        FieldDescriptor.JavaType.INT -> {
          when (value) {
            is Int -> value
            is Number -> value.toInt()
            is String -> value.toIntOrNull()
            is Boolean -> if (value) 1 else 0
            else -> null
          }
        }

        FieldDescriptor.JavaType.LONG -> {
          when (value) {
            is Long -> value
            is Number -> value.toLong()
            is String -> value.toLongOrNull()
            is Boolean -> if (value) 1L else 0L
            else -> null
          }
        }

        FieldDescriptor.JavaType.FLOAT -> {
          when (value) {
            is Float -> value
            is Number -> value.toFloat()
            is String -> value.toFloatOrNull()
            is Boolean -> if (value) 1f else 0f
            else -> null
          }
        }

        FieldDescriptor.JavaType.DOUBLE -> {
          when (value) {
            is Double -> value
            is Number -> value.toDouble()
            is String -> value.toDoubleOrNull()
            is Boolean -> if (value) 1.0 else 0.0
            else -> null
          }
        }

        FieldDescriptor.JavaType.BOOLEAN -> {
          when (value) {
            is Boolean -> value
            is String -> value.toBoolean()
            is Number -> value.toInt() != 0
            else -> null
          }
        }

        FieldDescriptor.JavaType.STRING -> value.toString()

        FieldDescriptor.JavaType.BYTE_STRING -> {
          when (value) {
            is ByteString -> value
            is ByteArray -> ByteString.copyFrom(value)
            is String -> ByteString.copyFromUtf8(value)
            else -> null
          }
        }

        else -> null
      }
    }
  }
}
