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
package com.adobe.analytics.zdw.format

import java.io._
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import scala.collection.JavaConverters._

import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.csv.CSVFormat

class ZDWStreamIteratorTest extends BasicSpec {
  // Read .desc.sql column names
  private[this] def readSQLColumnNames(file: File): Seq[String] = {
    new BufferedReader(new FileReader(file))
      .lines()
      .iterator()
      .asScala
      .toSeq
      .map(_.split('\t').head)
  }

  // Read .sql TSV expected rows
  private[this] def readSQLValuesFile(file: File): Iterator[Seq[String]] = {
    CSVFormat.newFormat('\t')
      .withRecordSeparator('\n')
      .withQuote('"')
      .withEscape('\\')
      .parse(new FileReader(file))
      .iterator()
      .asScala
      .map { record =>
        record.iterator().asScala.toSeq.map { value =>
          // Trim padded floats and doubles
          if (value.matches("[0-9]+\\.[0-9]+")) {
            value.toDouble.toString
          } else {
            value
          }
        }
      }
  }

  // Recreate the formatting unconvertDWfile would have done
  private[this] val sqlValueDateFormat = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.setTimeZone(TimeZone.getTimeZone("UTC"))
    format
  }
  private[this] def toSQLValueString(value: Any): String = {
    value match {
      case isNull if isNull == null => ""
      case dateTime: Date => sqlValueDateFormat.format(dateTime)
      case big: BigInt => big.toString(10)
      case other => other.toString
    }
  }

  // Used for compressed GZ and XZ test-files
  private[this] val compressorStreamFactory = new CompressorStreamFactory()

  // Build a set of tests and expected results from test-files directory
  private[this] val testFilesDir = new File("../test-files")
  private[this] val tests = testFilesDir.list(new FilenameFilter {
    override def accept(dir: File, name: String): Boolean = {
      !name.startsWith(".") && !name.startsWith("_") && (name.endsWith(".zdw") || name.contains(".zdw."))
    }
  }).map { filename =>
    val extPos = filename.indexOf(".zdw")
    val basename = filename.substring(0, extPos)

    val getExpectedColumnNames = () => {
      readSQLColumnNames(new File(testFilesDir, basename + ".desc.sql"))
    }
    val getExpectedSQLValues = () => {
      readSQLValuesFile(new File(testFilesDir, basename + ".sql"))
    }
    val getStream = () => {
      val fileStream = new FileInputStream(new File(testFilesDir, filename))
      new DataInputStream(if (filename.endsWith(".gz")) {
        compressorStreamFactory.createCompressorInputStream(CompressorStreamFactory.GZIP, fileStream)
      } else if (filename.endsWith(".xz")) {
        compressorStreamFactory.createCompressorInputStream(CompressorStreamFactory.XZ, fileStream)
      } else {
        fileStream
      })
    }

    (filename, getExpectedColumnNames, getExpectedSQLValues, getStream)
  }

  describe("ZDWStreamIterator") {
    it("should be able to read and match unconvertDWfile output from test-files") {
      tests.foreach {
        case (filename, getExpectedColumnNames, getExpectedSQLValues, getStream) => {
          println(scala.Console.CYAN + s"Reading and comparing $filename" + scala.Console.RESET)
          val stream = getStream()
          try {
            val expectedColumnNames = getExpectedColumnNames()
            val testIterator = ZDWStreamIterator(getStream())
            val testColumns = testIterator.columns()
            val testColumnNames = testColumns.map(_.name)
            testColumnNames should be(expectedColumnNames)
            val testRowsSQL = testIterator.map(_.map(toSQLValueString))
            testRowsSQL.zip(getExpectedSQLValues()).zipWithIndex.foreach {
              case ((row: Seq[String], expectedRow: Seq[String]), rowNum: Int) =>
                testColumnNames.zip(row).zip(expectedColumnNames.zip(expectedRow)).zipWithIndex.foreach {
                  case (((columnName, columnValue), (expectedColumnName, expectedColumnValue)), columnNum) =>
                    withClue(s"$filename:\nRow[$rowNum][$columnNum][$columnName / $expectedColumnName]: ") {
                      columnName should be(expectedColumnName)
                      columnValue should be(expectedColumnValue)
                    }
                }
            }
          } finally {
            stream.close()
          }
        }
      }
    }

    it("should be able to read and match unconvertDWfile output from test-files with specific columns") {
      tests.foreach {
        case (filename, getExpectedColumnNames, getExpectedSQLValues, getStream) => {
          println(scala.Console.YELLOW + s"Reading and comparing $filename with specific columns" + scala.Console.RESET)
          val stream = getStream()
          try {
            val expectedColumnNames = getExpectedColumnNames()
            // Select even columns
            val specificColumns = expectedColumnNames.zipWithIndex.flatMap {
              case (columnName, index) if index % 2 == 0 => Some(columnName)
              case _ => None
            }
            val testIterator = ZDWStreamIterator(getStream(), specificColumns = Some(specificColumns.toList))
            val testColumns = testIterator.columns()
            val testColumnNames = testColumns.map(_.name)
            testColumnNames should be(specificColumns)
            val testRowsSQL = testIterator.map(_.map(toSQLValueString))
            testRowsSQL.zip(getExpectedSQLValues()).zipWithIndex.foreach {
              case ((row: Seq[String], expectedFullRow: Seq[String]), rowNum: Int) =>
                val expectedRow = expectedFullRow.zipWithIndex.flatMap {
                  case (value, index) if index % 2 == 0 => Some(value)
                  case _ => None
                }
                testColumnNames.zip(row).zip(specificColumns.zip(expectedRow)).zipWithIndex.foreach {
                  case (((columnName, columnValue), (expectedColumnName, expectedColumnValue)), columnNum) =>
                    withClue(s"$filename spec-cols:\nRow[$rowNum][$columnNum][$columnName / $expectedColumnName]: ") {
                      columnName should be(expectedColumnName)
                      columnValue should be(expectedColumnValue)
                    }
                }
            }
          } finally {
            stream.close()
          }
        }
      }
    }
  }
}
