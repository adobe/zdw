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
package com.adobe.analytics.zdw.file

import java.io.{Closeable, DataInputStream, File, FileInputStream}
import java.net.{URI, URISyntaxException}
import java.nio.charset.{Charset, StandardCharsets}

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.net.ftp.FTPClient

import com.adobe.analytics.zdw.format.{ZDWBlock, ZDWColumn, ZDWStreamIterator}

case class ZDWFileReader(
  path: String,
  charset: Charset = StandardCharsets.UTF_8,
  specificColumns: Option[Seq[String]] = None
) extends Iterator[Seq[Any]] with Closeable with LazyLogging {

  private[this] val compressorStreamFactory = new CompressorStreamFactory()

  private[this] var ftpClientKey: (String, Option[Int], Option[String], Option[String]) = _
  private[this] var ftpClient: FTPClient = _

  private[this] lazy val streamIterator = {
    val fileStream = try {
      val uri = new URI(path)
      // Handle FTP URI or local file
      if (uri.getScheme != null && uri.getScheme.equalsIgnoreCase("ftp")) {
        ftpClientKey = FTPPool.buildKey(uri)
        val ftpClient = FTPPool.borrowClient(ftpClientKey)
        ftpClient.retrieveFileStream(uri.getPath)
      } else {
        new FileInputStream(new File(path))
      }
    } catch {
      case syntaxError: URISyntaxException => new FileInputStream(new File(path))
    }

    val stream = new DataInputStream(if (path.endsWith(".gz")) {
      compressorStreamFactory.createCompressorInputStream(CompressorStreamFactory.GZIP, fileStream)
    } else if (path.endsWith(".xz")) {
      compressorStreamFactory.createCompressorInputStream(CompressorStreamFactory.XZ, fileStream)
    } else {
      fileStream
    })

    ZDWStreamIterator(stream, charset, specificColumns)
  }

  def columns(): Seq[ZDWColumn] = streamIterator.columns()
  def block(): ZDWBlock = streamIterator.block()

  override def hasNext: Boolean = streamIterator.hasNext

  override def next(): Seq[Any] = streamIterator.next()

  override def close(): Unit = {
    streamIterator.close()
    if (ftpClient != null) {
      FTPPool.returnClient(ftpClientKey, ftpClient)
    }
  }
}

object ZDWFileReader extends LazyLogging {
  class Builder {
    private[this] var _charset: Option[Charset] = None
    private[this] var _path: Option[String] = None
    private[this] var _specificColumns: Option[Seq[String]] = None

    def withCharset(charset: Charset): Builder = {
      _charset = Some(charset)
      this
    }
    def withPath(path: String): Builder = {
      _path = Some(path)
      this
    }
    def withSpecificColumns(specificColumns: Seq[String]): Builder = {
      _specificColumns = Some(specificColumns)
      this
    }

    def build(): ZDWFileReader = {
      val charset = _charset.getOrElse(StandardCharsets.UTF_8)
      val path = _path.getOrElse(throw new IllegalArgumentException("path not provided"))
      val specificColumns = _specificColumns.filter(_.nonEmpty)

      ZDWFileReader(path, charset, specificColumns)
    }
  }

  def newBuilder(): Builder = {
    new Builder
  }

  def closeConnections(): Unit = {
    FTPPool.close()
  }
}
