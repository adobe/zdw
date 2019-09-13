# ZDW File Format

The ZDW archival format is for row-level, well-structured data (i.e., TSV with a schema file, as from a MySQL dump) that
can be used in tandem with standard compression formats to yield highly efficient compression.
It is best suited for optimizing storage footprint for archival storage,
accessing large segments of the data, and outputting data row-by-row, as opposed to extracting only a few columns.

ZDW uses a combination of:

* A global, sorted dictionary of unique strings across all text columns
* Numeric and text values, as specified in an accompanying SQL-like schema file
* Variable byte-size values for integers as well as dictionary indexes
* Minimum value baseline per column for integers and dictionary indexes, to reduce the magnitude of the value needed on each row of that column
* Bit-flagging repeat column values across consecutive rows (similar approach to run-length encoding, but applied on a per-row basis)

It has evolved through multiple internal iterations (at version 10 as of September 2018).
See [ZDW v10 format](ZDW%20v10%20format.png) for a diagram of the ZDW v10 format.

## Approach

The ZDW compressor performs two passes over the uncompressed data.
The first pass compiles, sorts and outputs a header, global string dictionary, per-column offset sizes and baseline values for numeric columns.
The second pass converts and outputs the compressed row-oriented data.
When a standard compression flag is applied, this output data is piped into the respective compression binary (e.g., 'xz', 'gz') for additional compression.

Multiple internal data blocks are supported in order to keep dictionary sizes manageable.
These blocks are for memory management and are not currently intended to provide intra-file seek optimizations.

## Efficiency

ZDW is designed to complement standard text or binary compression algorithms layered on top.

As ZDW is best suited for highly efficient compression of long-term archival data,
the current recommended compression to apply on top of the ZDW file format is XZ (LZMA).
XZ incurs a one-time compute-intensive compression cost, then has a reasonable compute cost for uncompressing one or more times.
Applying XZ compression is enabled by supplying the '-J' option to the compressor.
Additional compressor command line options, such as invoking parallel XZ compression on large datasets, can be applied
via an additional '--zargs' command line option to the compressor, e.g., "--zargs='-7 --threads=2'".

Less efficient formats like GZ (gzip) may alternatively be used,
with a positive trade-off of reducing compression compute time and requiring about
half the CPU cycles for uncompression compared to XZ,
while incurring an anticipated reduction in compression efficiency of 25%.

Expected compression efficiency is substantially higher than alternative formats
like [ORC](https://orc.apache.org/specification/ORCv1/) and [Parquet](https://parquet.apache.org/documentation/latest/).

Sample benchmark data, to illustrate expected efficiency compressing files with varying schema widths and properties, can be found in the [test-files](test-files) directory.

## Data Types

ZDW understands the data-type of each column.  These types are based on
SQL column types.  The list of supported types are:

| Data Type | Internal ID | Java/Scala Type | Spark Sql Type |
|-----------|-------------|-----------------|----------------|
| VARCHAR | 0 | String | StringType |
| TEXT | 1 | String | StringType |
| DATETIME | 2 | java.util.Date in UTC timezone | TimestampType |
| CHAR_2 | 3 | String | StringType |
| CHAR | 6 | String | StringType |
| TINY | 7 | Short | ShortType |
| SHORT | 8 | Int | IntegerType |
| LONG | 9 | Long | LongType |
| LONG LONG | 10 | BigInt | StringType (no big-int in Spark SQL) |
| DECIMAL | 11 | Double | DoubleType |
| TINY SIGNED | 12 | Byte | ByteType |
| SHORT SIGNED | 13 | Short | ShortType |
| LONG SIGNED | 14 | Int | IntegerType |
| LONG LONG SIGNED | 15 | Long | LongType |
| TINYTEXT | 16 | String | StringType |
| MEDIUMTEXT | 17 | String | StringType |
| LONGTEXT | 18 | String | StringType |

For more details see [ZDWColumn](format/src/main/scala/com/adobe/analytics/zdw/format/ZDWColumn.scala).

## Access Patterns

For access patterns, ZDW best caters to:

* Decompress an entire file's contents, either to disk, streamed to stdout, or unpacked into a row-level in-memory text buffer
* Access data row-by-row
* Access multiple columns in all rows

## Creating ZDW files

Currently this project is providing write support only via the cplusplus/convertDWfile binary.

### Example usage
A TSV file with a ".sql" extension must reside on disk alongside a ".desc.sql" file containing the schema of the ".sql" file.

Run "./convertDWfile infile.sql" to create a ZDW file of the specified input file.

### Command line parameters

Include a '-J' argument to compress ZDW files with XZ.
Include '-v' to validate the created file with cmp (to confirm the uncompressed ZDW data is byte-for-byte identical to the source file).

Run without arguments to view usage and all supported ZDW file creation options.

## Read Support

For reading, this project provides the following interfaces.

### System binaries

cplusplus/unconvertDWfile binary

### C++ interface

A C++ library API (see [test_unconvert_api.cpp](cplusplus/test_unconvert_api.cpp) for example)

### Generic Format Definition and DataInputStream Reader

The first read layer provided works on a generic DataInputStream.  It doesn't
handle the filesystem or compression for you, but you can use any Java/Scala
filesystem or compression tools on top of this that work with DataInputStream.

See [format](format) for more details.

```xml
<dependency>
  <groupId>com.adobe.analytics.zdw</groupId>
  <artifactId>zdw-format</artifactId>
  <version>0.1.0</version>
</dependency>
```

### File Reader Using Apache Commons

The second read layer provided uses the Java filesystem implementation and
Apache Commons tools to provide local and FTP server filesystem support as
well as GZIP and XZ (LZMA) compression support.

```xml
<dependency>
  <groupId>com.adobe.analytics.zdw</groupId>
  <artifactId>zdw-file</artifactId>
  <version>0.1.0</version>
</dependency>
```

See [file](file) for more details
and [here](file/src/test/scala/com/adobe/analytics/zdw/file/ZDWFileReaderTest.scala)
for example usage.

### File Reader Using Hadoop

The third read layer is an alternative to the above implementation using
Apache Commons.  Instead it used Hadoop to provide the filesystem and
compression support so you can use any filesystem or compression supported
by your Hadoop environment.

```xml
<dependency>
  <groupId>com.adobe.analytics.zdw</groupId>
  <artifactId>zdw-hadoop</artifactId>
  <version>0.1.0</version>
</dependency>
```

See [hadoop](hadoop) for more details
and [here](hadoop/src/test/scala/com/adobe/analytics/zdw/hadoop/ZDWFileReaderTest.scala)
for example usage.

### Spark SQL FileFormat

The last read layer is a Spark SQL FileFormat that builds on the Hadoop
layer so can use any filesystem or compression supported by your
Hadoop/Spark environment.

```xml
<dependency>
  <groupId>com.adobe.analytics.zdw</groupId>
  <artifactId>zdw-spark-sql</artifactId>
  <version>0.1.0</version>
</dependency>
```

See [spark-sql](spark-sql) for more details
and [here](spark-sql/src/test/scala/com/adobe/analytics/zdw/spark/sql/ZDWFileFormatTest.scala)
for example usage.
