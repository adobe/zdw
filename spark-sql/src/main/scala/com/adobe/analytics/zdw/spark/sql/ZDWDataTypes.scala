/**
 * Copyright 2019 Adobe. All rights reserved.
 * This file is licensed to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package com.adobe.analytics.zdw.spark.sql

import java.util.Date

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.adobe.analytics.zdw.format.ZDWColumn

private[sql] object ZDWDataTypes {
  import ZDWColumn._

  val dataTypes: Map[Short, DataType] = Map(
    DATA_TYPE_UNKNOWN -> StringType,
    DATA_TYPE_VARCHAR -> StringType,
    DATA_TYPE_TEXT -> StringType,
    DATA_TYPE_DATETIME -> TimestampType,
    DATA_TYPE_CHAR_2 -> StringType,
    DATA_TYPE_CHAR -> StringType,
    DATA_TYPE_TINY -> ShortType,
    DATA_TYPE_SHORT -> IntegerType,
    DATA_TYPE_LONG -> LongType,
    DATA_TYPE_LONGLONG -> StringType, // Not ideal, but Spark doesn't have a BigInt type
    DATA_TYPE_DECIMAL -> DoubleType,
    DATA_TYPE_TINY_SIGNED -> ByteType,
    DATA_TYPE_SHORT_SIGNED -> ShortType,
    DATA_TYPE_LONG_SIGNED -> IntegerType,
    DATA_TYPE_LONGLONG_SIGNED -> LongType,
    DATA_TYPE_TINYTEXT -> StringType,
    DATA_TYPE_MEDIUMTEXT -> StringType,
    DATA_TYPE_LONGTEXT -> StringType
  )

  def convert(dataType: DataType, value: Any): Any = (dataType, value) match {
    // case dateTime: Date => new Timestamp(dateTime.getTime)
    case (_, null) if value == null => null
    case (StringType, bigInt: BigInt) => UTF8String.fromString(bigInt.toString(10)) // Again...not ideal
    case (StringType, _) => UTF8String.fromString(value.toString)
    case (TimestampType, dateTime: Date) => dateTime.getTime * 1000
    case (ByteType, byte: Byte) => byte
    case (ByteType, short: Short) => short.toByte
    case (ByteType, int: Int) => int.toByte
    case (ByteType, long: Long) => long.toByte
    case (ShortType, byte: Byte) => byte.toShort
    case (ShortType, short: Short) => short
    case (ShortType, int: Int) => int.toShort
    case (ShortType, long: Long) => long.toShort
    case (IntegerType, byte: Byte) => byte.toInt
    case (IntegerType, short: Short) => short.toInt
    case (IntegerType, int: Int) => int.toInt
    case (IntegerType, long: Long) => long.toInt
    case (LongType, byte: Byte) => byte.toLong
    case (LongType, short: Short) => short.toLong
    case (LongType, int: Int) => int.toLong
    case (LongType, long: Long) => long.toLong
    case (FloatType, float: Float) => float
    case (FloatType, double: Double) => double.toFloat
    case (DoubleType, float: Float) => float.toDouble
    case (DoubleType, double: Double) => double
    case (_, _) => value
  }
}
