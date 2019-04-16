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

import java.sql.Timestamp

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll

import com.adobe.analytics.zdw.file.UsageSpec

abstract class SparkSpec extends UsageSpec with BeforeAndAfterAll {
  private var _sparkSession: SparkSession = null
  protected def getSparkSession: SparkSession = _sparkSession

  override def beforeAll(): Unit = {
    if (_sparkSession == null) {
      _sparkSession = SparkSession.builder()
        .appName(getClass.getCanonicalName)
        .master("local[2]")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    }
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      if (_sparkSession != null) {
        _sparkSession.sparkContext.stop()
        _sparkSession = null
      }
    } finally {
      super.afterAll()
    }
  }

  protected def checkRow(row: Row, expectedValues: Seq[Any]): Unit = {
    row.schema.fields.indices.foreach(i => row.schema.fields(i).dataType match {
      case ByteType => row.get(i).asInstanceOf[Byte] should be(expectedValues(i).asInstanceOf[Byte])
      case ShortType => row.get(i).asInstanceOf[Short] should be(expectedValues(i).asInstanceOf[Short])
      case IntegerType => row.get(i).asInstanceOf[Int] should be(expectedValues(i).asInstanceOf[Int])
      case LongType => row.get(i).asInstanceOf[Long] should be(expectedValues(i).asInstanceOf[Long])
      case StringType => row.getString(i) should be(expectedValues(i).asInstanceOf[String])
      case TimestampType => row.get(i).asInstanceOf[Timestamp] should be(expectedValues(i).asInstanceOf[Timestamp])
    })
  }
}
