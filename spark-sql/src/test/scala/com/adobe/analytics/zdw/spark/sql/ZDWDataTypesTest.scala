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

class ZDWDataTypesTest extends SparkSpec {
  describe("ZDWDataTypes") {
    it("should be able to convert based on spark and scala types") {
      def ct[T](dataType: DataType, value: Any)(validate: T => Unit): Unit = {
        val cv = ZDWDataTypes.convert(dataType, value).asInstanceOf[T]
        validate(cv)
      }

      ct[String](StringType, null)(_ should be(null))
      ct[UTF8String](StringType, "foo")(_.toString should be("foo"))

      val date = new Date()
      val ts = date.getTime
      ct[Long](TimestampType, date)(_ should be(ts * 1000))

      ct[Byte](ByteType, 127.toByte)(_ should be(127.toByte))
      ct[Byte](ByteType, 127.toShort)(_ should be(127.toByte))
      ct[Byte](ByteType, 127)(_ should be(127.toByte))
      ct[Byte](ByteType, 127L)(_ should be(127.toByte))
      ct[Short](ShortType, 127.toByte)(_ should be(127.toShort))
      ct[Short](ShortType, 1024.toShort)(_ should be(1024.toShort))
      ct[Short](ShortType, 1024)(_ should be(1024.toShort))
      ct[Short](ShortType, 1024L)(_ should be(1024.toShort))
      ct[Int](IntegerType, 127.toByte)(_ should be(127))
      ct[Int](IntegerType, 1024.toShort)(_ should be(1024))
      ct[Int](IntegerType, 1000000)(_ should be(1000000))
      ct[Int](IntegerType, 1000000L)(_ should be(1000000))
      ct[Long](LongType, 127.toByte)(_ should be(127L))
      ct[Long](LongType, 1024.toShort)(_ should be(1024L))
      ct[Long](LongType, 1000000)(_ should be(1000000L))
      ct[Long](LongType, 6000000000L)(_ should be(6000000000L))

      ct[Float](FloatType, 1.25F)(_ should be(1.25F))
      ct[Float](FloatType, 1.25D)(_ should be(1.25F))
      ct[Double](DoubleType, 100.25F)(_ should be(100.25D))
      ct[Double](DoubleType, 100.25D)(_ should be(100.25D))
    }
  }
}
