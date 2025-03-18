//package org.wfanet.measurement.securecomputation.teeapps.utils
//
//import com.google.protobuf.ByteString
//import com.google.protobuf.Descriptors.EnumValueDescriptor
//import com.google.protobuf.Descriptors.Descriptor
//import com.google.protobuf.Descriptors.FieldDescriptor
//import com.google.protobuf.DynamicMessage
//import com.google.protobuf.Message
//
//
//
//interface FieldMapper {
//  fun getMappedValue(sourceValue: Any): Any
//}
//
//data class FieldDescriptorAndMapper(
//  val fieldDescriptor: FieldDescriptor,
//  val mapper: FieldMapper,
//)
//
//
//
//class ProtobufMessageConverter(
//  private val fieldMapping: Map<String, Map<String, FieldDescriptorAndMapper>>
//) {
//  fun convertMessage(
//    originalMessage: Message,
//    newMessageDescriptor: Descriptor,
//  ): DynamicMessage {
//    val builder = DynamicMessage.newBuilder(newMessageDescriptor)
//
//    for (field in originalMessage.allFields) {
//      val fieldName = field.key.name
//      // TODO: dont just continue if these values arent there. throw an error because this is not expected. the values should be present
//      val (newMessageFieldDescriptor, fieldMapper) = fieldMapping[newMessageDescriptor.fullName]!![fieldName] ?: continue
//      val newValue = fieldMapper.getMappedValue(field.value)
//      val convertedValue = convertValueToTargetType(newValue, newMessageFieldDescriptor) ?:
//      throw IllegalArgumentException("Failed to convert value for field ${fieldName} from ${newValue.javaClass} to ${newMessageFieldDescriptor.javaType}")
//      builder.setField(newMessageFieldDescriptor, convertedValue)
//    }
//
//    return builder.build()
//  }
//
//  /**
//   * Converts a value to the appropriate type for the target field.
//   */
//  private fun convertValueToTargetType(value: Any, fieldDescriptor: FieldDescriptor): Any? {
//    return when (fieldDescriptor.javaType) {
//      FieldDescriptor.JavaType.MESSAGE -> {
//        when (value) {
//          // If value is already a Message, check if it's compatible
//          is Message -> {
//            if (value.descriptorForType == fieldDescriptor.messageType) {
//              // Already the correct type
//              value
//            } else {
//              // Would need custom conversion between different message types
//              null
//            }
//          }
//          else -> null
//        }
//      }
//
//      FieldDescriptor.JavaType.ENUM -> {
//        when (value) {
//          // If value is already an EnumValueDescriptor, check if it belongs to the right enum type
//          is EnumValueDescriptor -> {
//            if (value.type == fieldDescriptor.enumType) value else null
//          }
//          // If value is a Java/Kotlin enum
//          is Enum<*> -> fieldDescriptor.enumType.findValueByName(value.name)
//          // If value is a string, try to find corresponding enum value
//          is String -> fieldDescriptor.enumType.findValueByName(value)
//          // If value is an integer, try to find corresponding enum value by number
//          is Int -> fieldDescriptor.enumType.findValueByNumber(value)
//          // If nothing else works, try toString and find by name
//          else -> fieldDescriptor.enumType.findValueByName(value.toString())
//        }
//      }
//
//      FieldDescriptor.JavaType.INT -> {
//        when (value) {
//          is Int -> value
//          is Number -> value.toInt()
//          is String -> value.toIntOrNull()
//          is Boolean -> if (value) 1 else 0
//          else -> null
//        }
//      }
//
//      FieldDescriptor.JavaType.LONG -> {
//        when (value) {
//          is Long -> value
//          is Number -> value.toLong()
//          is String -> value.toLongOrNull()
//          is Boolean -> if (value) 1L else 0L
//          else -> null
//        }
//      }
//
//      FieldDescriptor.JavaType.FLOAT -> {
//        when (value) {
//          is Float -> value
//          is Number -> value.toFloat()
//          is String -> value.toFloatOrNull()
//          is Boolean -> if (value) 1f else 0f
//          else -> null
//        }
//      }
//
//      FieldDescriptor.JavaType.DOUBLE -> {
//        when (value) {
//          is Double -> value
//          is Number -> value.toDouble()
//          is String -> value.toDoubleOrNull()
//          is Boolean -> if (value) 1.0 else 0.0
//          else -> null
//        }
//      }
//
//      FieldDescriptor.JavaType.BOOLEAN -> {
//        when (value) {
//          is Boolean -> value
//          is String -> value.toBoolean()
//          is Number -> value.toInt() != 0
//          else -> null
//        }
//      }
//
//      FieldDescriptor.JavaType.STRING -> value.toString()
//
//      FieldDescriptor.JavaType.BYTE_STRING -> {
//        when (value) {
//          is ByteString -> value
//          is ByteArray -> ByteString.copyFrom(value)
//          is String -> ByteString.copyFromUtf8(value)
//          else -> null
//        }
//      }
//
//      else -> null
//    }
//  }
//}
//
