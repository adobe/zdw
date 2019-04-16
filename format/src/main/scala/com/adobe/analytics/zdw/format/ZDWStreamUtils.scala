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
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.Charset

import scala.collection.mutable

private[format] object ZDWStreamUtils {
  def readUShort(bytes: Array[Byte]): Int = {
    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getShort() & 0xFFFF
  }
  def readUShort(stream: DataInputStream): Int = {
    val bytes = new Array[Byte](2)
    stream.readFully(bytes, 0, 2)
    readUShort(bytes)
  }

  def readUInt(bytes: Array[Byte]): Long = {
    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt() & 0xFFFFFFFFL
  }
  def readUInt(stream: DataInputStream): Long = {
    val bytes = new Array[Byte](4)
    stream.readFully(bytes, 0, 4)
    readUInt(bytes)
  }

  def readULong(bytes: Array[Byte]): Long = {
    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong()
  }
  def readULong(stream: DataInputStream): Long = {
    val bytes = new Array[Byte](8)
    stream.readFully(bytes, 0, 8)
    readULong(bytes)
  }

  def readBig(bytes: Array[Byte]): BigInt = {
    BigInt(bytes.reverse)
  }

  def readBigLong(stream: DataInputStream): BigInt = {
    val bytes = new Array[Byte](8)
    stream.readFully(bytes, 0, 8)
    readBig(bytes)
  }

  def readVarBig(stream: DataInputStream, numBytes: Int): BigInt = {
    val bytes = Array[Byte](0, 0, 0, 0, 0, 0, 0, 0)
    stream.readFully(bytes, 0, numBytes)
    readBig(bytes)
  }
  def readVarBig(stream: DataInputStream): BigInt = {
    readVarBig(stream, stream.readByte())
  }

  def readStrings(stream: DataInputStream, charset: Charset): Seq[String] = {
    val bytes = mutable.Buffer[Byte]()
    var byte = (-1).toByte
    var prevByte = (-1).toByte
    while (byte != 0 || prevByte != 0) {
      prevByte = byte
      byte = stream.readByte()
      bytes.append(byte)
    }
    new String(bytes.toArray, charset)
      .split('\0')
      .filter(_.nonEmpty)
  }
}
