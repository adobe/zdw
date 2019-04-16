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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class ZDWFileFormatTest extends SparkSpec {
  private[this] val expectedTestValues = Seq(
    Seq("Jane", 2),
    Seq("Jane", 1),
    Seq("John", 2),
    Seq("John", 1)
  )
  describe("ZDWFileFormat") {
    it("should be able to read local files into a DataFrame") {
      val sparkSession = getSparkSession
      Seq(
        s"$testFilesPath/test.zdw",
        s"$testFilesPath/test.zdw.gz",
        s"$testFilesPath/test.zdw.xz"
      ).foreach { path =>
        println("===========================")
        println(path)

        val df = sparkSession.read
          .format("com.adobe.analytics.zdw.spark.sql.ZDWFileFormat")
          .option(ZDWOptions.CONF_CHARSET, "UTF-8")
          .load(path)
          .select(col("firstName"), col("eventCode"))
          .orderBy(col("firstName").asc, col("eventCode").desc)
        df.printSchema()
        df.schema.fields should be(Seq(
          StructField("firstName", StringType),
          StructField("eventCode", IntegerType)
        ))
        df.show(truncate = false)
        val collected = df.collect()
        collected.length should be(4)
        for (i <- collected.indices) {
          checkRow(collected(i), expectedTestValues(i))
        }
      }
    }

    it("should be able to read FTP files into a DataFrame") {
      val sparkSession = getSparkSession
      Seq(
        s"ftp://$ftpServerUsername:$ftpServerPassword@localhost:$ftpServerPort$ftpServerPath/test.zdw",
        s"ftp://$ftpServerUsername:$ftpServerPassword@localhost:$ftpServerPort$ftpServerPath/test.zdw.gz",
        s"ftp://$ftpServerUsername:$ftpServerPassword@localhost:$ftpServerPort$ftpServerPath/test.zdw.xz"
      ).foreach { path =>
        println("===========================")
        println(path)

        val df = sparkSession.read
          .format("com.adobe.analytics.zdw.spark.sql.ZDWFileFormat")
          .option(ZDWOptions.CONF_CHARSET, "UTF-8")
          .load(path)
          .select(col("firstName"), col("eventCode"))
          .orderBy(col("firstName").asc, col("eventCode").desc)
        df.printSchema()
        df.schema.fields should be(Seq(
          StructField("firstName", StringType),
          StructField("eventCode", IntegerType)
        ))
        df.show(truncate = false)
        val collected = df.collect()
        collected.length should be(4)
        for (i <- collected.indices) {
          checkRow(collected(i), expectedTestValues(i))
        }
      }
    }
  }
}
