package org.wfanet.measurement.securecomputation.teeapps.utils

import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors
import com.google.protobuf.Descriptors.EnumValueDescriptor
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import com.google.protobuf.kotlin.toByteStringUtf8

class TeeApplicationUtils {
  companion object {
    fun convertMessage(
      originalMessage: Message,
      newMessageDescriptor: Descriptor,
      fieldNameMap: Map<String, String>,
      fieldValueMap: Map<String, String>
    ): DynamicMessage {
      val builder = DynamicMessage.newBuilder(newMessageDescriptor)
      for ((originalFieldPath, newFieldPath) in fieldNameMap) {
        val originalFieldValue = getFieldValueByPath(originalMessage, originalFieldPath)
        val newFieldDescriptor = getFieldDescriptorByPath(newMessageDescriptor, newFieldPath)
        if (originalFieldValue != null && newFieldDescriptor != null) {
          val convertedValue = convertValue(originalFieldValue, newFieldDescriptor, fieldValueMap, originalFieldPath)
          builder.setField(newFieldDescriptor, convertedValue)
        }
      }
      return builder.build()
    }
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
    private fun getFieldDescriptorByPath(descriptor: Descriptor, fieldPath: String): FieldDescriptor? {
      val fieldNames = fieldPath.split(".")
      var currentDescriptor: Descriptor = descriptor
      for (fieldName in fieldNames.dropLast(1)) {
        val fieldDescriptor = currentDescriptor.findFieldByName(fieldName)
        if (fieldDescriptor != null && fieldDescriptor.type == FieldDescriptor.Type.MESSAGE) {
          currentDescriptor = fieldDescriptor.messageType
        } else {
          return null
        }
      }
      return currentDescriptor.findFieldByName(fieldNames.last())
    }
    private fun convertValue(
      originalValue: Any,
      newFieldDescriptor: FieldDescriptor,
      fieldValueMap: Map<String, String>,
      originalFieldPath: String
    ): Any {
      val originalValueString = originalValue.toString()
      val mappedValueKey = "$originalFieldPath.$originalValueString"
      val mappedValueString = fieldValueMap[mappedValueKey] ?: originalValueString
      return when (newFieldDescriptor.type) {
        FieldDescriptor.Type.INT32 -> mappedValueString.toInt()
        FieldDescriptor.Type.INT64 -> mappedValueString.toLong()
        FieldDescriptor.Type.FLOAT -> mappedValueString.toFloat()
        FieldDescriptor.Type.DOUBLE -> mappedValueString.toDouble()
        FieldDescriptor.Type.BOOL -> mappedValueString.toBoolean()
        FieldDescriptor.Type.STRING -> mappedValueString
        FieldDescriptor.Type.ENUM -> newFieldDescriptor.enumType.findValueByName(mappedValueString)
        else -> originalValue // Handle other types as needed
      }
    }
  }
}
