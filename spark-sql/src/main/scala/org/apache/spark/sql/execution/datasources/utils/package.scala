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
package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.types.StructType

/**
 * There are still required utilities for DataSources that Spark hasn't
 * moved to the public interface yet.
 */
package object utils {
  def appendPartitionColumns(
    partitionSchema: StructType,
    requiredSchema: StructType,
    file: PartitionedFile,
    iter: Iterator[InternalRow]
  ): Iterator[InternalRow] = {

    val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
    val joinedRow = new JoinedRow()
    val appendPartitionColumns = GenerateUnsafeProjection.generate(fullSchema, fullSchema)

    if (partitionSchema.length == 0) {
      // There is no partition columns
      iter.asInstanceOf[Iterator[InternalRow]]
    } else {
      iter.asInstanceOf[Iterator[InternalRow]]
        .map(d => appendPartitionColumns(joinedRow(d, file.partitionValues)))
    }
  }

}
