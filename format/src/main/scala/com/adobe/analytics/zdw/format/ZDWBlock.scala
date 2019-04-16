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

import java.io.DataInputStream
import java.nio.charset.{Charset, StandardCharsets}
import java.text.SimpleDateFormat

import scala.collection.mutable

import com.typesafe.scalalogging.slf4j.LazyLogging

case class ZDWBlockColumn(
  private[format] val valueNumBytes: Byte,
  private[format] val minValue: Long,
  private[format] val minValueBig: BigInt,

  private[format] val newValueFlagByteNum: Int,
  private[format] val newValueFlagMask: Short
) {
  val hasValues: Boolean = valueNumBytes != 0
}
case class ZDWBlock(
  numRows: Long,
  private[format] val maxRowSize: Long,
  isFinal: Boolean,
  columns: Seq[ZDWBlockColumn],
  private[format] val dictionary: ZDWDictionary,

  private[format] val stream: DataInputStream = null,
  private[format] val header: ZDWHeader,
  private[format] val specificColumnNums: Option[Seq[Int]]
) extends Iterator[Seq[Any]] with LazyLogging {

  private[this] val rowNumColumns = specificColumnNums.map(_.size).getOrElse(header.columns.size)
  private[this] val rowColumnIndexes = specificColumnNums.map({ columnNums =>
    header.columns.indices
      .map(columnNum => columnNum -> columnNums.indexOf(columnNum))
      .filter(_._2 >= 0)
  }).getOrElse(header.columns.indices.map(columnNum => columnNum -> columnNum))
    .toMap


  private[this] var rowNum: Int = 0

  private[this] val numColumnsUsed = columns.count(_.hasValues)
  private[this] val newValueFlagsNumBytes = Math.ceil(numColumnsUsed.toDouble / 8.0d).toInt
  private[this] val newValueFlagsBytes = new Array[Byte](newValueFlagsNumBytes)
  private[this] val prevColumnValues = new Array[Any](header.columns.size)

  private[this] val dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  override def hasNext: Boolean = rowNum < numRows

  override def next(): Seq[Any] = {
    import ZDWStreamUtils._
    import ZDWColumn._

    if (hasNext) {
      // Fill row with defaults to handle specified columns that may not be in this file
      val row = mutable.ArrayBuffer.fill[Any](rowNumColumns)(defaultValues(DATA_TYPE_UNKNOWN))

      // Read in bit flags for reusing the previous column values
      stream.readFully(newValueFlagsBytes, 0, newValueFlagsNumBytes)
      // scalastyle:off
      // println(s"[ZDW][ROW][$rowNum] (size = ${newValueFlagsNumBytes}) ${newValueFlagsBytes.take(8).map(b => "%02X".format(b)).mkString(" ")}")
      // scalastyle:on
      // Read each column
      header.columns.indices.foreach { columnNum =>
        val column = header.columns(columnNum)
        val blockColumn = columns(columnNum)

        val columnName = column.name
        val rowIndex = rowColumnIndexes.get(columnNum)
        val dataType = column.dataType
        val valueNumBytes = blockColumn.valueNumBytes
        val hasValues = blockColumn.hasValues
        val minValue = blockColumn.minValue
        val minValueBig = blockColumn.minValueBig
        val newValueFlagByteNum = blockColumn.newValueFlagByteNum
        val newValueFlagMask = blockColumn.newValueFlagMask

        val valueTyped = if (hasValues) {
          // The bit 1 means you need a new value
          val newValue = newValueFlagsBytes(newValueFlagByteNum) & newValueFlagMask

          if (newValue != 0) {
            val valueBig = readVarBig(stream, valueNumBytes)
            val valueBigWithMin = if (valueBig != 0) valueBig + minValueBig else valueBig
            val value = valueBig.toLong
            val valueWithMin = if (value != 0) value + minValue else value
            // println(s"[ZDW][ROW][$rowNum][$columnNum] type = $dataType, readValue = $value, minValue = $minValue")
            prevColumnValues(columnNum) = {
              dataType match {
                case DATA_TYPE_VARCHAR |
                  DATA_TYPE_TEXT |
                  DATA_TYPE_DATETIME |
                  DATA_TYPE_CHAR_2 |
                  DATA_TYPE_TINYTEXT |
                  DATA_TYPE_MEDIUMTEXT |
                  DATA_TYPE_LONGTEXT |
                  DATA_TYPE_DECIMAL => {
                  if (value != 0) {
                    val dictionaryIndex = valueWithMin
                    val ref = dictionary.getRef(dictionaryIndex.toInt)
                    // println(s"[ZDW][ROW][$rowNum][$columnName / $columnNum][REF] index = ${ref._2}")
                    if (dataType == DATA_TYPE_DECIMAL) {
                      val refString = ZDWDictionary.refToString(StandardCharsets.UTF_8, ref)
                      if (refString.nonEmpty) {
                        try {
                          refString.toDouble
                        } catch {
                          case t: Throwable =>
                            logger.error(s"[ZDW][ROW][$columnName] bad double value $refString - $ref", t)
                            defaultValues(dataType)
                        }
                      } else {
                        defaultValues(dataType)
                      }
                    } else if (dataType == DATA_TYPE_DATETIME) {
                      val refString = ZDWDictionary.refToString(StandardCharsets.UTF_8, ref)
                      if (refString.nonEmpty) {
                        try {
                          dateTimeFormat.parse(refString)
                        } catch {
                          case t: Throwable =>
                            logger.error(s"[ZDW][ROW][$columnName] bad date-time value $refString - $ref", t)
                            defaultValues(dataType)
                        }
                      } else {
                        defaultValues(dataType)
                      }
                    } else {
                      ref
                    }
                  } else {
                    defaultValues(dataType)
                  }
                }

                case DATA_TYPE_CHAR => {
                  if (value != 0) {
                    val charTuple = valueWithMin.toInt
                    val char = (charTuple & 0xFF).toChar
                    // Single character
                    if (char != '\\') {
                      if (char != '\0') {
                        char.toString
                      } else {
                        defaultValues(dataType)
                      }
                    // Escape character + escaped character
                    } else {
                      val escapedChar = ((charTuple >> 8) & 0xFF).toChar
                      new String(Array(char, escapedChar))
                    }
                  } else {
                    defaultValues(dataType)
                  }
                }

                // Use one size bigger to deal with unsigned
                case DATA_TYPE_TINY => valueWithMin.toShort
                case DATA_TYPE_SHORT => valueWithMin.toInt
                case DATA_TYPE_LONG => valueWithMin

                // Too big for a long
                case DATA_TYPE_LONGLONG => valueBigWithMin

                // Use the expected size
                case DATA_TYPE_TINY_SIGNED => valueWithMin.toByte
                case DATA_TYPE_SHORT_SIGNED => valueWithMin.toShort
                case DATA_TYPE_LONG_SIGNED => valueWithMin.toInt
                case DATA_TYPE_LONGLONG_SIGNED => valueWithMin
              }
            }
          } else {
            // println(s"[ZDW][ROW][$rowNum][$columnName / $columnNum][PREV] ${prevColumnValues(columnNum)}")
          }
          if (prevColumnValues(columnNum) != null) {
            prevColumnValues(columnNum)
          } else {
            // println(s"[ZDW][ROW][$rowNum][$columnName / $columnNum][DEFAULT-FOR-NULL] ${defaultValues(dataType)}")
            defaultValues(dataType)
          }
        } else {
          // println(s"[ZDW][ROW][$rowNum][$columnName / $columnNum][DEFAULT] ${defaultValues(dataType)}")
          defaultValues(dataType)
        }

        rowIndex.foreach(index => row(index) = valueTyped)
      }

      rowNum = rowNum + 1

      row
    } else {
      null
    }
  }
}

object ZDWBlock extends LazyLogging {
  def apply(
    charset: Charset,
    stream: DataInputStream,
    header: ZDWHeader,
    specificColumnNums: Option[Seq[Int]]
  ): ZDWBlock = {
    import ZDWStreamUtils._
    import ZDWColumn._

    val numRows = readUInt(stream)
    logger.debug(s"[ZDW][BLOCK] numRows = $numRows")
    val maxRowSize = readUInt(stream)
    logger.debug(s"[ZDW][BLOCK] maxRowSize = $maxRowSize")
    val isFinal = stream.readByte() != 0
    logger.debug(s"[ZDW][BLOCK] isFinal = $isFinal")
    val dictionary = ZDWDictionary(charset, stream, header)
    var usedColumnNum = 0
    val columns = header.columns.indices
      .map(columnNum => stream.readByte())
      .map(valueNumBytes => {
        val (minValue, minValueBig, newValueFlagByteNum, newValueFlagMask) = if (valueNumBytes > 0) {
          val big = readBigLong(stream)
          val byteNum = usedColumnNum / 8
          val mask = (1 << (usedColumnNum % 8)).toShort
          usedColumnNum = usedColumnNum + 1
          (big.toLong, big, byteNum, mask)
        } else {
          (0L, defaultBig, -1, 0.toShort)
        }
        ZDWBlockColumn(valueNumBytes, minValue, minValueBig, newValueFlagByteNum, newValueFlagMask)
      })
    logger.debug(s"[ZDW][BLOCK] columns =\n\t${header.columns.zip(columns).mkString("\n\t")}")

    ZDWBlock(
      numRows,
      maxRowSize,
      isFinal,
      columns,
      dictionary,

      stream,
      header,
      specificColumnNums
    )
  }
}
