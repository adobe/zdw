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

import java.io.{Closeable, DataInputStream}
import java.nio.charset.{Charset, StandardCharsets}

case class ZDWStreamIterator(
  stream: DataInputStream,
  charset: Charset = StandardCharsets.UTF_8,
  specificColumns: Option[Seq[String]] = None
) extends Iterator[Seq[Any]] with Closeable {

  import ZDWColumn._

  private[this] lazy val header = ZDWHeader(charset, stream)
  private[this] lazy val columnIndexes = header.columns.zipWithIndex.map({
    case (column, index) => column.nameLowerCase -> index
  }).toMap
  private[this] lazy val specificColumnNums = specificColumns.map(_.map { columnName =>
    columnIndexes.getOrElse(columnName.toLowerCase, -1)
  })
  private[this] lazy val specificColumnDetails = specificColumns.map(_.map { columnName =>
    columnIndexes.get(columnName.toLowerCase)
      .map(columnNum => header.columns(columnNum))
      .getOrElse(ZDWColumn(columnName.toLowerCase, DATA_TYPE_UNKNOWN, 0))
  })

  def columns(): Seq[ZDWColumn] = specificColumnDetails.getOrElse(header.columns)
  def block(): ZDWBlock = currentBlock

  private[format] var currentBlock: ZDWBlock = _
  private[format] def updateCurrentBlock(): Unit = {
    if (currentBlock == null || (!currentBlock.hasNext && !currentBlock.isFinal)) {
      currentBlock = ZDWBlock(
        charset,
        stream,
        header,
        specificColumnNums
      )
    }
  }

  override def hasNext: Boolean = {
    if (usingBlockIterator) {
      throw new IllegalStateException("Row iteration not allowed in block mode")
    }

    updateCurrentBlock()
    currentBlock.hasNext || !currentBlock.isFinal
  }

  override def next(): Seq[Any] = {
    if (usingBlockIterator) {
      throw new IllegalStateException("Row iteration not allowed in block mode")
    }

    updateCurrentBlock()
    if (hasNext) {
      val rawRow = currentBlock.next()
      rawRow.map {
        // TODO: Based on option return raw vs doing dictionary extraction and character set conversion for Spark
        case ref: (Array[Byte], Int, Int) =>
          ZDWDictionary.refToString(charset, ref)
        case other => other
      }
    } else {
      null
    }
  }

  override def close(): Unit = {
    stream.close()
  }

  /**
   * Alternate iterator interface that works a block at a time
   * providing both the block and the column definitions with
   * hasValues set based on the block column details.
   */
  private[this] var usingBlockIterator = false
  def blockIterator: Iterator[(ZDWBlock, Seq[ZDWColumn])] = {
    usingBlockIterator = true
    new Iterator[(ZDWBlock, Seq[ZDWColumn])] {
      override def hasNext: Boolean = {
        currentBlock == null || !currentBlock.isFinal
      }

      override def next(): (ZDWBlock, Seq[ZDWColumn]) = {
        if (currentBlock != null && currentBlock.hasNext) {
          throw new IllegalStateException("Unread block rows")
        }
        updateCurrentBlock()
        if (currentBlock.hasNext || currentBlock.numRows == 0) {
          // Get the specified block columns
          val blockColumns = specificColumnNums.map(_.map({ columnNum =>
            if (columnNum >= 0) {
              currentBlock.columns(columnNum)
            } else {
              null
            }
          })).getOrElse(currentBlock.columns)
          // Merge with specified file columns
          val mergedColumns = columns().zip(blockColumns).map {
            case (column, blockColumn) => column.copy(hasValues = Option(blockColumn).exists(_.hasValues))
          }
          // Hand back new block and the merged column details
          (currentBlock, mergedColumns)
        } else {
          null
        }
      }
    }
  }
}
