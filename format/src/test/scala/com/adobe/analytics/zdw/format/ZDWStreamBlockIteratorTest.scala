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

import java.io.{DataInputStream, File, FileInputStream}

class ZDWStreamBlockIteratorTest extends BasicSpec {
  describe("ZDWStreamBlockIterator") {
    it("should be able to iterate over blocks and provide column details") {
      val fileStream = new FileInputStream(new File("../test-files/analytics-hits.zdw"))
      val stream = new DataInputStream(fileStream)
      val streamIterator = ZDWStreamIterator(stream)
      val blockIterator = streamIterator.blockIterator

      // Read blocks
      blockIterator.foreach { case (block, columns) =>
        println("==================================================")
        println(s"Block: numRows = ${block.numRows}, isFinal = ${block.isFinal}, columns:")
        val columnNotes = columns
          .map(column => s"\t${column.name}: dataType = ${column.dataType}, hasValues = ${column.hasValues}")
        val noteFormat = "%-" + columnNotes.maxBy(_.length).length + "s"
        columnNotes
          .map(note => noteFormat.format(note))
          .grouped(3)
          .map(_.mkString(" - "))
          .foreach(println)

        // Spot check a couple columns
        columns.find(_.name == "export_version").foreach(_.hasValues should be(true))
        columns.find(_.name == "m_evar178").foreach(_.hasValues should be(false))

        // Read in and check the size of the rows
        block.foreach { row =>
          withClue("row value count vs file column count") {
            row.size should be(streamIterator.columns().size)
          }
          withClue("row value count vs block column count") {
            row.size should be(columns.size)
          }
        }
      }
    }
  }
}
