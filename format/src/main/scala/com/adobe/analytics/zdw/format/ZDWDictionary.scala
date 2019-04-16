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

import scala.collection.mutable

import com.typesafe.scalalogging.slf4j.LazyLogging

case class ZDWDictionary(
  private[format] val numBytes: Long,
  private[format] val bytes: Array[Byte]
) {
  private[this] val nullOffsets = mutable.Map[Int, Int]()

  def getRef(index: Int): (Array[Byte], Int, Int) = {
    // Find and cache the null terminators by index
    val nullOffset = nullOffsets.getOrElseUpdate(index, bytes.indexOf(0, index))
    (bytes, index, nullOffset)
  }
}

object ZDWDictionary extends LazyLogging {
  def apply(charset: Charset, stream: DataInputStream, header: ZDWHeader): ZDWDictionary = {
    import ZDWStreamUtils._

    val numBytes = readVarBig(stream).toInt
    val bytes = new Array[Byte](numBytes)
    logger.debug(s"[ZDW][DICTIONARY] numBytes = $numBytes / ${bytes.length}")
    if (numBytes > 0) {
      stream.readFully(bytes, 0, numBytes)
      // scalastyle:off
      // logger.debug(s"[ZDW][DICTIONARY] dictionary =\n\t${new String(bytes).split('\0').grouped(6).map(_.mkString("\t")).mkString("\n\t")}")
      // scalastyle:on
    }
    ZDWDictionary(numBytes, bytes)
  }

  def refToString(charset: Charset, ref: (Array[Byte], Int, Int)): String = {
    if (ref._3 > ref._2) {
      new String(ref._1, ref._2, ref._3 - ref._2, charset)
    } else {
      ZDWColumn.defaultString
    }
  }
}
