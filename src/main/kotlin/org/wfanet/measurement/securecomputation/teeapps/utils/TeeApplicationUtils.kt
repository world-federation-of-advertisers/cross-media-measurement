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
  }
}
