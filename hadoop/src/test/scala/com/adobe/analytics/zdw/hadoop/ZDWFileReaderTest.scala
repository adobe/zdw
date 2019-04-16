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
package com.adobe.analytics.zdw.hadoop

import java.nio.charset.StandardCharsets

import org.apache.hadoop.fs.Path

import com.adobe.analytics.zdw.file.UsageSpec

class ZDWFileReaderTest extends UsageSpec {
  describe("ZDWFileReader") {
    it("should be able to read local files using hadoop") {
      Seq(
        new Path(s"$testFilesPath/test.zdw"),
        new Path(s"$testFilesPath/test.zdw.gz"),
        new Path(s"$testFilesPath/test.zdw.xz")
      ).foreach { path =>
        println("===========================")
        println(path)
        println("---------------------------")

        val reader = ZDWFileReader.newBuilder()
          .withCharset(StandardCharsets.UTF_8)
          .withSpecificColumns(Seq("firstName", "age"))
          .withPath(path)
          .build()

        try {
          reader.columns().map(_.name) should be(Seq("firstName", "age"))

          reader.columns().foreach(column => print("| %10s ".format(column.name)))
          println("|\n---------------------------")
          var count = 0
          reader.foreach { row =>
            row.foreach(value => print("| %10s ".format(value.toString)))
            println("|")

            row.size should be(2)
            count = count + 1
          }
          count should be(4)
          println("---------------------------\n")
        } finally {
          reader.close()
        }
      }
    }

    it("should be able to read files from FTP server using hadoop") {
      Seq(
        new Path(s"ftp://$ftpServerUsername:$ftpServerPassword@localhost:$ftpServerPort$ftpServerPath/test.zdw"),
        new Path(s"ftp://$ftpServerUsername:$ftpServerPassword@localhost:$ftpServerPort$ftpServerPath/test.zdw.gz"),
        new Path(s"ftp://$ftpServerUsername:$ftpServerPassword@localhost:$ftpServerPort$ftpServerPath/test.zdw.xz")
      ).foreach { path =>
        println("===========================")
        println(path)
        println("---------------------------")

        val reader = ZDWFileReader.newBuilder()
          .withCharset(StandardCharsets.UTF_8)
          .withSpecificColumns(Seq("firstName", "age"))
          .withPath(path)
          .build()

        try {
          reader.columns().map(_.name) should be(Seq("firstName", "age"))

          reader.columns().foreach(column => print("| %10s ".format(column.name)))
          println("|\n---------------------------")
          var count = 0
          reader.foreach { row =>
            row.foreach(value => print("| %10s ".format(value.toString)))
            println("|")

            row.size should be(2)
            count = count + 1
          }
          count should be(4)
          println("---------------------------\n")
        } finally {
          reader.close()
        }
      }
    }
  }
}
