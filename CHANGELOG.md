# Amazon Neptune JDBC Driver Change Log

## v3.1.0 Change Log

`v3.1.0` of the **Amazon Neptune JDBC Driver** will build on `v3.0.0` but has not been started yet.

## v3.0.0 Change Log

`v3.0.0` of the **Amazon Neptune JDBC Driver** is a major update release which provides compatibility with Neptune engine version >= 1.2.0.0. 

Note, this version is not compatible with engine version <1.2.0.0. If you are using previous version, please refer to the [Compatibility Matrix](README.md#compatibility-with-aws-neptune) to find what JDBC driver version to choose.

### Bug Fixes
* Fixed issues with openCypher IAM auth signature with Neptune engine version 1.2.0.0
* Fixed build failures related to checkstyle versions

### New Features and Improvements
* Refactored SQL-Gremlin submodule into main module for publishing artifacts onto Maven 

## v2.0.0 Change Log

`v2.0.0` of the **Amazon Neptune JDBC Driver** is a major update release which provides compatibility with Neptune engine version 1.1.1.0+.

Additional bug fixes and new features are listed below:

### Bug Fixes
* Fixed issues with openCypher IAM auth signature with Neptune engine version 1.1.1.0 
* Fixed `GROUP BY` with renamed table
* Fixed bug in getting column names for metadata
 
### New Features and Improvements
* Added `X-Amz-Security-Token` to support session token
* Added basic literal comparisons in `WHERE` and `HAVING` after `INNER JOIN`
* Added `NOT` support to `WHERE` clause
* Added support for scalar values without column names

## v1.1.0 Change Log

`v1.1.0` of the **Amazon Neptune JDBC Driver** builds on `v1.0.0`. 

This release provides bug fixes and a new features, details are listed below:

### Bug Fixes
* Fixed issue with `LIMIT 1` queries in DBVisualizer
* Fixed results for `GROUP BY` and `WHERE` in aggregates
* Fixed bugs in SSH tunnel
* Fixed bug with `JOIN` on vertices of different labels
* Fixed metadata caching to be on database url basis
* Fixed Log4j security issue (CVE-2021-44228 and CVE-2021-45046)
* Fixed issue with edge mismatch in `JOIN` returning results
* Fixed `ORDER BY` column on column that has `null` values
* Fixed issue with aggregate filtering on `null` valued columns
* Fixed 'false positive' exception log produced on statement shutdown

### New Features and Improvements
* Removed Janino jar for revision control and update Calcite
* Metadata overhaul
  * Driver version
  * getUrl
  * getTypeInfo
  * Removed incorrect catalog support
  * Fixed incorrect values reported in metadata
* Added Maven central hookup for Gradle publishing
* Moved sql-gremlin errors to resources
* Updated taco file to support SSH tunnel
* Improved error messages in sql-gremlin
* Added comparator support in SELECT clause
* Updated dialect file for taco to remove `NULLS FIRST`/`LAST` in `ORDER BY`

### Documentation Enhancements
* Added documentation for DBeaver
* Added documentation around schema collection
* Enhanced documentation for JOIN queries
* Added more connection string and ssh tunnel examples in documentation


## v1.0.0 Change Log

`v1.0.0` is the first official GA release of the **Amazon Neptune JDBC Driver**.

This release includes a bunch of improvements and new features that are listed below:

* Bug fixes and enhanced support for SQL to Gremlin conversion
    * `HAVING` support
    * `ORDER BY` using label
    * `COUNT(*)` support
    * Edge column retrieval
    * Various other minor improvements
* Tableau extract mode enabled
* Tableau data preview enabled
* `SERVICE_REGION` now supported as a connection property
* Cut down the output size of the shadow jar
* Fixes to enhance JDBC metadata
* Enhanced documentation
* Tableau connector updates to support SERVICE_REGION

## v1.0.0-beta

The Amazon Neptune JDBC Driver is a JDBC 4.2 compliant driver (Java 8), which provides read-only JDBC connectivity for the Amazon Neptune service using graph query languages Gremlin, openCypher and SPARQL, as well as SQL.

When using SQL, the graph is represented in a table/columnar format and SQL queries can be executed. The driver supports a subset of SQL-92 along with some common extensions and supports SELECT statements of the general form:

```
SELECT [ DISTINCT ] { * | <projectItem> [, <projectItem> ]* }
   FROM <tableExpression>
   [ WHERE <booleanExpression> ]
   [ GROUP BY { <column> [, <column> ]* } ]
   [ ORDER BY { <column> [ DESC ] [, <column> [ DESC ] ]* } ]
   [ LIMIT limitNumber ]

projectItem:
    [ agg ]* [ <table>. ]* <column> [ [ AS ] columnAlias ]
```

The driver can be used to integrate with BI tools that support JDBC Drivers. For Tableau Desktop users a connector is available to use.
