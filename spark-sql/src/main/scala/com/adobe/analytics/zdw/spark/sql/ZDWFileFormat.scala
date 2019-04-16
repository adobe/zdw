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
package com.adobe.analytics.zdw.spark.sql

import java.net.URI
import java.nio.charset.Charset

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.execution.datasources.utils._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{StructField, StructType}

import com.adobe.analytics.zdw.format.ZDWColumn
import com.adobe.analytics.zdw.hadoop.ZDWFileReader

class ZDWFileFormat extends FileFormat
  with DataSourceRegister
  with LazyLogging
  with Serializable {

  import ZDWFileFormat._

  override def shortName(): String = "zdw"

  override def toString: String = "ZDW"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[ZDWFileFormat]

  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = false

  override def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = false

  override def inferSchema(
    sparkSession: SparkSession,
    options: Map[String, String],
    files: Seq[FileStatus]
  ): Option[StructType] = {
    val zdwOptions = ZDWOptions(options)

    // If we're not merging schemas just use the first file
    val filesToTouch = if (!zdwOptions.mergeSchema) {
      Seq(files.head)
    } else {
      files
    }

    val readers = filesToTouch.map { status =>
      buildZDWReader(
        sparkSession.sessionState.newHadoopConf(),
        zdwOptions,
        status.getPath
      )
    }

    val distinctColumns = readers
      .foldLeft(Seq.empty[ZDWColumn])((columnsBuffer, reader) => {
        val ret = columnsBuffer ++ reader.columns
        reader.close()
        ret
      })
      .distinct

    Some(StructType(distinctColumns.map(column => StructField(column.name, ZDWDataTypes.dataTypes(column.dataType)))))
  }

  override def prepareWrite(
    sparkSession: SparkSession,
    job: Job,
    options: Map[String, String],
    dataSchema: StructType
  ): OutputWriterFactory = {
    throw new UnsupportedOperationException("ZDW write is not supported")
  }

  override def buildReaderWithPartitionValues(
    sparkSession: SparkSession,
    dataSchema: StructType,
    partitionSchema: StructType,
    requiredSchema: StructType,
    filters: Seq[Filter],
    options: Map[String, String],
    hadoopConf: Configuration
  ): (PartitionedFile) => Iterator[InternalRow] = {

    val zdwOptions = ZDWOptions(options)

    val broadcastedHadoopConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    // Build the set of columns to read
    val specificColumns = if (requiredSchema.length > 0) {
      Some(requiredSchema.fieldNames.toSeq)
    } else {
      None
    }

    // Build the function for reating this file/partition
    (file: PartitionedFile) => {
      val path = new Path(new URI(file.filePath))
      val reader = buildZDWReader(
        broadcastedHadoopConf.value.value,
        zdwOptions,
        path,
        specificColumns
      )

      // Build converter from GenericInternalRow to UnsafeRow
      val fullSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
      val unsafeProjection = UnsafeProjection.create(fullSchema)

      val rowIterator = new Iterator[InternalRow] {
        override def hasNext: Boolean = reader.hasNext

        override def next(): InternalRow = {
          val zdwRow = reader.next()
          val row = InternalRow.fromSeq(zdwRow.zip(fullSchema.fields).map(
            entry => ZDWDataTypes.convert(entry._2.dataType, entry._1)
          ))
          // Auto-close when we're out of rows
          if (!reader.hasNext) {
            reader.close()
          }
          // Convert from GenericInternalRow to UnsafeRow
          unsafeProjection(row)
        }
      }

      // Update iterator to append Partition fields to rows if they are present
      appendPartitionColumns(
        partitionSchema,
        requiredSchema,
        file,
        rowIterator
      )
    }
  }
}

object ZDWFileFormat {
  def buildZDWReader(
    hadoopConf: Configuration,
    zdwOptions: ZDWOptions,
    path: Path,
    specificColumns: Option[Seq[String]] = None
  ): ZDWFileReader = {

    val readerBuilder = ZDWFileReader.newBuilder()
      .withConf(hadoopConf)
      .withPath(path)

    val readerBuilderWithCharset = zdwOptions.charsetName
      .foldLeft(readerBuilder) { (b, charsetName) =>
        b.withCharset(Charset.forName(charsetName))
      }

    val readerBuilderWithSpecificColumns = specificColumns
      .foldLeft(readerBuilderWithCharset) { (b, columns) =>
        b.withSpecificColumns(columns)
      }

    readerBuilderWithSpecificColumns.build()
  }
}
