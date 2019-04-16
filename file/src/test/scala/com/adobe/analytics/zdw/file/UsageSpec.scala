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

import java.io._
import java.net.ServerSocket

import org.mockftpserver.fake.{FakeFtpServer, UserAccount}
import org.mockftpserver.fake.filesystem.{DirectoryEntry, FileEntry, FileSystemEntry, UnixFakeFileSystem}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers, OptionValues}

abstract class UsageSpec extends FunSpec with Matchers with OptionValues with BeforeAndAfterAll {
  private[this] def getFreePort(from: Int, to: Int): Int = {
    var port = from
    var done = false
    while (!done && port <= to) {
      done = try {
        new ServerSocket(port).close()
        true
      } catch {
        case e: IOException =>
          port = port + 1
          false
      }
    }
    if (!done) {
      throw new IOException("failed to find open port")
    }
    port
  }

  protected[this] val testFilesDir = new File("../test-files")
  protected[this] val testFilesPath = testFilesDir.getCanonicalPath
  protected[this] val testFilenames = testFilesDir.list(new FilenameFilter {
    override def accept(dir: File, name: String): Boolean = {
      !name.startsWith(".") && !name.startsWith("_") && (name.endsWith(".zdw") || name.contains(".zdw."))
    }
  })

  protected[this] lazy val ftpServerPort = getFreePort(5000, 5999)
  protected[this] val ftpServerUsername = "user1"
  protected[this] val ftpServerPassword = "pass1"
  protected[this] val ftpServerPath = testFilesPath
  private[this] var ftpServer: FakeFtpServer = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    ftpServer = {
      val server = new FakeFtpServer()
      server.setServerControlPort(ftpServerPort)
      server.addUserAccount(new UserAccount(ftpServerUsername, ftpServerPassword, ftpServerPath))
      val fs = new UnixFakeFileSystem() {
        override def getEntry(path: String): FileSystemEntry = {
          val entry = super.getEntry(path)
          // println(s"[FTP Server] Find entry $path -> $entry")
          entry
        }
      }
      fs.add(new DirectoryEntry(ftpServerPath))
      testFilenames.foreach { filename =>
        val testFile = new File(testFilesDir, filename)
        val filenamePath = testFile.getCanonicalPath
        println(s"[FTP Server] Adding $filenamePath")
        fs.add(new FTPRealFileEntry(filenamePath, testFile))
      }
      server.setFileSystem(fs)
      server.start()
      server
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()

    if (ftpServer != null && !ftpServer.isShutdown) {
      ftpServer.stop()
    }
  }
}

// Internally the fake FTP server FileEntry only uses a string buffer for contents.  We want to use the actual file.
private[file] class FTPRealFileEntry(path: String, file: File) extends FileEntry(path) {
  override def getSize(): Long = file.length()

  override def createInputStream(): InputStream = {
    // println(s"[FTP Server] Getting fake FTP server stream for $path -> ${file.getCanonicalPath}")
    new FileInputStream(file)
  }

  override def cloneWithNewPath(newPath: String): FileSystemEntry = {
    // println(s"[FTP Server] Cloning $path to $newPath")
    val clone = new FTPRealFileEntry(newPath, file)
    clone.setLastModified(getLastModified)
    clone.setOwner(getOwner)
    clone.setGroup(getGroup)
    clone.setPermissions(getPermissions)
    clone
  }
}
