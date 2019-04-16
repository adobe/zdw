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
package org.apache.spark.sql.execution.datasources.utils

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging

// scalastyle:off
/**
 * This is pretty much a copy of the internal Spark Hadoop utility because
 * they haven't opened it up yet.
 *
 * @see https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/SerializableConfiguration.scala
 */
// scalastyle:on
class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    value = new Configuration(false)
    value.readFields(in)
  }
}

private[this] object Utils extends Logging {
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }
}


