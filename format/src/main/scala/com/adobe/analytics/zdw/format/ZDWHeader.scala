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
import java.nio.charset.Charset

import com.typesafe.scalalogging.slf4j.LazyLogging

case class ZDWHeader(
  version: Int,
  columns: Seq[ZDWColumn]
)

object ZDWHeader extends LazyLogging {
  def apply(charset: Charset, stream: DataInputStream): ZDWHeader = {
    import ZDWStreamUtils._

    val version = readUShort(stream)
    logger.debug(s"[ZDW][HEADER] version = $version")
    if (version != 9 && version != 10) {
      throw new IllegalArgumentException(s"unsupported ZDW version $version")
    }
    val columnNames = readStrings(stream, charset)
    val columns = columnNames
      .map(columnName => (columnName, stream.readByte()))
      .map(columnInfo => ZDWColumn(columnInfo._1, columnInfo._2, readUShort(stream)))
    logger.debug(s"[ZDW][HEADER] columns =\n\t${columns.mkString("\n\t")}")
    ZDWHeader(version, columns)
  }
}
