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

import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets

import scala.collection.mutable

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.commons.net.ftp.FTPClient
import org.apache.commons.pool2.{ObjectPool, PooledObject, PooledObjectFactory}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}

private[file] object FTPPool extends LazyLogging {
  def buildKey(uri: URI): (String, Option[Int], Option[String], Option[String]) = {
    val port = if (uri.getPort > 0) Some(uri.getPort) else None
    val (username, password) = if (uri.getUserInfo != null) {
      val parts = uri.getUserInfo.split(':')
      val username = URLDecoder.decode(parts(0), StandardCharsets.UTF_8.name())
      if (username.nonEmpty) {
        if (parts.length > 1) {
          val password = URLDecoder.decode(parts(1), StandardCharsets.UTF_8.name())
          (Some(username), Some(password))
        } else {
          (Some(username), None)
        }
      } else {
        (None, None)
      }
    } else {
      (None, None)
    }
    (uri.getHost, port, username, password)
  }

  private[this] val ftpClientPools = mutable.HashMap[
    (String, Option[Int], Option[String], Option[String]),
    ObjectPool[FTPClient]
  ]()

  private[file] def borrowClient(
    key: (String, Option[Int], Option[String], Option[String])
  ): FTPClient = synchronized {

    ftpClientPools.getOrElseUpdate(key, buildPool(key)).borrowObject()
  }

  private[file] def returnClient(
    key: (String, Option[Int], Option[String], Option[String]),
    ftpClient: FTPClient
  ): Unit = {
    ftpClientPools.get(key).foreach(_.returnObject(ftpClient))
  }

  private[this] class FTPClientPooledObjectFactory(
    key: (String, Option[Int], Option[String], Option[String])
  ) extends PooledObjectFactory[FTPClient] {

    override def makeObject(): PooledObject[FTPClient] = {
      val ftpClient = new FTPClient()
      ftpClient.connect(key._1, key._2.getOrElse(21))
      ftpClient.login(
        key._3.getOrElse(throw new IllegalArgumentException("FTP username required")),
        key._4.getOrElse(throw new IllegalArgumentException("FTP password required"))
      )
      new DefaultPooledObject[FTPClient](ftpClient)
    }

    override def destroyObject(pooledObject: PooledObject[FTPClient]): Unit = {
      if (pooledObject.getObject.isConnected) {
        try {
          pooledObject.getObject.logout()
        } catch {
          case t: Throwable => logger.warn(s"Failed to logout of ${key._1}:${key._2}:${key._3}")
        }
        pooledObject.getObject.disconnect()
      }
    }

    override def validateObject(pooledObject: PooledObject[FTPClient]): Boolean = true

    override def activateObject(pooledObject: PooledObject[FTPClient]): Unit = {}

    override def passivateObject(pooledObject: PooledObject[FTPClient]): Unit = {}
  }

  private[this] def buildPool(
    key: (String, Option[Int], Option[String], Option[String])
  ): ObjectPool[FTPClient] = {
    new GenericObjectPool[FTPClient](new FTPClientPooledObjectFactory(key))
  }

  def close(): Unit = {
    ftpClientPools.values.foreach { ftpClientPool =>
      ftpClientPool.close()
    }
  }

}
