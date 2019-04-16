# Test Data Files and Compression Comparison

# Web Site Analytics Hits

This is an example of a very wide table with a few thousand rows that is
sparsely populated and has string values that are shared between columns.
ZDW's shared dictionary has a strong impact in this case.

| File Size  | Filename                      | Type           | Percent of Original |
|-----------:|-------------------------------|----------------|--------------------:|
| 14,468,990 | analytics-hits.sql            | TSV            | Original |
|  1,895,648 | analytics-hits.parquet        | Parquet        | 13.1% |
|  1,689,359 | analytics-hits.orc            | ORC            | 11.7% |
|  1,303,440 | analytics-hits.zdw            | ZDW            |  9.0% |
|  1,159,669 | analytics-hits.snappy.parquet | Parquet+Snappy |  8.0% |
|    944,057 | analytics-hits.gzip.parquet   | Parquet+GZIP   |  6.5% |
|    940,332 | analytics-hits.lzo.orc        | ORC+LZO        |  6.5% |
|    937,876 | analytics-hits.snappy.orc     | ORC+Snappy     |  6.5% |
|    667,348 | analytics-hits.sql.gz         | TSV+GZIP       |  4.6% |
|    631,981 | analytics-hits.zlib.orc       | ORC+ZLIB       |  4.4% |
|    398,388 | analytics-hits.sql.xz         | TSV+XZ         |  2.8% |
|    312,272 | analytics-hits.zdw.gz         | ZDW+GZIP       |  2.2% |
|    227,456 | analytics-hits.zdw.xz         | ZDW+XZ         |  1.6% |

# Movie Ticket Sales

This is an example of a narrow table where string
values are not shared between columns.
That is, there are a single-digit number of columns
and each column has its own distinct set of values.

| File Size  | Filename                      | Type           | Percent of Original |
|-----------:|-------------------------------|----------------|--------------------:|
| 32,653,800 | movie_tickets.sql             | TSV            | Original |
| 18,290,691 | movie_tickets.zdw             | ZDW            | 56.0% |
|  8,736,936 | movie_tickets.parquet         | Parquet        | 26.8% |
|  6,854,324 | movie_tickets.orc             | ORC            | 21.0% |
|  5,466,961 | movie_tickets.snappy.parquet  | Parquet+Snappy | 16.7% |
|  4,365,887 | movie_tickets.zdw.gz          | ZDW+GZIP       | 13.4% |
|  4,099,301 | movie_tickets.gzip.parquet    | Parquet+GZIP   | 12.6% |
|  3,913,324 | movie_tickets.sql.gz          | TSV+GZIP       | 12.0% |
|  3,697,046 | movie_tickets.lzo.orc         | ORC+LZO        | 11.3% |
|  3,619,216 | movie_tickets.snappy.orc      | ORC+Snappy     | 11.1% |
|  2,732,214 | movie_tickets.zlib.orc        | ORC+ZLIB       |  8.4% |
|  2,668,936 | movie_tickets.sql.xz          | TSV+XZ         |  8.2% |
|  2,040,472 | movie_tickets.zdw.xz          | ZDW+XZ         |  6.2% |
