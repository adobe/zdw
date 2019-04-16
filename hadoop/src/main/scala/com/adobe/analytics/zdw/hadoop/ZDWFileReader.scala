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

import java.io.{Closeable, DataInputStream}
import java.nio.charset.{Charset, StandardCharsets}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._

import com.adobe.analytics.zdw.format.{ZDWBlock, ZDWColumn, ZDWStreamIterator}

case class ZDWFileReader(
  conf: Configuration,
  path: Path,
  charset: Charset = StandardCharsets.UTF_8,
  specificColumns: Option[Seq[String]] = None
) extends Iterator[Seq[Any]] with Closeable {
  private[this] lazy val streamIterator = {
    val fs = path.getFileSystem(conf)
    val fileStream = fs.open(path)

    val compressionConf = Option(conf.get("io.compression.codecs")).getOrElse("")
    if (path.getName.endsWith(".xz") && !compressionConf.contains("XZCodec")) {
      conf.set("io.compression.codecs", "io.sensesecure.hadoop.xz.XZCodec," + compressionConf)
    }
    val compressionCodecs = new CompressionCodecFactory(conf)
    val stream = Option(compressionCodecs.getCodec(path))
      .map(codec => new DataInputStream(codec.createInputStream(fileStream)))
      .getOrElse(fileStream)

    ZDWStreamIterator(stream, charset, specificColumns)
  }

  def columns(): Seq[ZDWColumn] = streamIterator.columns()
  def block(): ZDWBlock = streamIterator.block()

  override def hasNext: Boolean = streamIterator.hasNext

  override def next(): Seq[Any] = streamIterator.next()

  override def close(): Unit = {
    streamIterator.close()
  }
}

object ZDWFileReader {
  class Builder {
    private[this] var _conf: Option[Configuration] = None
    private[this] var _charset: Option[Charset] = None
    private[this] var _path: Option[Path] = None
    private[this] var _specificColumns: Option[Seq[String]] = None

    def withConf(conf: Configuration): Builder = {
      _conf = Some(conf)
      this
    }
    def withCharset(charset: Charset): Builder = {
      _charset = Some(charset)
      this
    }
    def withPath(path: Path): Builder = {
      _path = Some(path)
      this
    }
    def withPath(path: String): Builder = {
      withPath(new Path(path))
    }
    def withSpecificColumns(specificColumns: Seq[String]): Builder = {
      _specificColumns = Some(specificColumns)
      this
    }

    def build(): ZDWFileReader = {
      val conf = _conf.getOrElse(new Configuration())
      val charset = _charset.getOrElse(StandardCharsets.UTF_8)
      val path = _path.getOrElse(throw new IllegalArgumentException("path not provided"))
      val specificColumns = _specificColumns.filter(_.nonEmpty)

      ZDWFileReader(conf, path, charset, specificColumns)
    }
  }

  def newBuilder(): Builder = {
    new Builder
  }
}
