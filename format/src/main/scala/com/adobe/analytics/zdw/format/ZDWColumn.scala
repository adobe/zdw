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

case class ZDWColumn(
  name: String,
  dataType: Short,
  private[format] val stringLength: Int, // Not used
  hasValues: Boolean = true // Only valid / used for block-iterator overlay of column details
) {
  lazy val nameLowerCase: String = name.toLowerCase

  override def hashCode(): Int = nameLowerCase.hashCode

  override def equals(obj: Any): Boolean = obj match {
    case column: ZDWColumn if column.nameLowerCase == nameLowerCase => true
    case other => false
  }
}

object ZDWColumn {
  val DATA_TYPE_UNKNOWN = (-1).toShort
  val DATA_TYPE_VARCHAR = 0.toShort
  val DATA_TYPE_TEXT = 1.toShort
  val DATA_TYPE_DATETIME = 2.toShort
  val DATA_TYPE_CHAR_2 = 3.toShort
  val DATA_TYPE_CHAR = 6.toShort
  val DATA_TYPE_TINY = 7.toShort
  val DATA_TYPE_SHORT = 8.toShort
  val DATA_TYPE_LONG = 9.toShort
  val DATA_TYPE_LONGLONG = 10.toShort
  val DATA_TYPE_DECIMAL = 11.toShort
  val DATA_TYPE_TINY_SIGNED = 12.toShort
  val DATA_TYPE_SHORT_SIGNED = 13.toShort
  val DATA_TYPE_LONG_SIGNED = 14.toShort
  val DATA_TYPE_LONGLONG_SIGNED = 15.toShort
  val DATA_TYPE_TINYTEXT = 16.toShort
  val DATA_TYPE_MEDIUMTEXT = 17.toShort
  val DATA_TYPE_LONGTEXT = 18.toShort

  val defaultString: String = null
  val defaultBig: BigInt = BigInt(0)
  def defaultValues: Map[Short, Any] = Map(
    DATA_TYPE_UNKNOWN -> defaultString,
    DATA_TYPE_VARCHAR -> defaultString,
    DATA_TYPE_TEXT -> defaultString,
    DATA_TYPE_DATETIME -> defaultString,
    DATA_TYPE_CHAR_2 -> defaultString,
    DATA_TYPE_CHAR -> defaultString,
    DATA_TYPE_TINY -> 0,
    DATA_TYPE_SHORT -> 0,
    DATA_TYPE_LONG -> 0,
    DATA_TYPE_LONGLONG -> BigInt(0L),
    DATA_TYPE_DECIMAL -> 0.0d,
    DATA_TYPE_TINY_SIGNED -> 0,
    DATA_TYPE_SHORT_SIGNED -> 0,
    DATA_TYPE_LONG_SIGNED -> 0,
    DATA_TYPE_LONGLONG_SIGNED -> 0L,
    DATA_TYPE_TINYTEXT -> defaultString,
    DATA_TYPE_MEDIUMTEXT -> defaultString,
    DATA_TYPE_LONGTEXT -> defaultString
  )
}
