# SQL Support and Limitations

The SQL-Gremlin compiler turns your SQL queries into Gremlin traversals and runs them against your favorite TinkerPop 3 enabled graph database.

## Basic Query Form

The driver current support `SELECT` statements of the general form:

    SELECT [ DISTINCT ] { * | <projectItem> [, <projectItem> ]* }
       FROM <tableExpression>
       [ WHERE <booleanExpression> ]
       [ GROUP BY { <column> [, <column> ]* } ]
       [ HAVING <booleanExpression> ]
       [ ORDER BY { <column> [ DESC ] [, <column> [ DESC ] ]* } ]
       [ LIMIT limitNumber ]

    projectItem:
        [ agg ]* [ <table>. ]* <column> [ [ AS ] columnAlias ]

Queries without a `FROM` clause or only using `VALUES` in the `FROM` clause are not supported.

A `tableExpression` must specify 1 or more tables as a comma separated list or using `JOIN` keywords. See the [Joins](#joins) section for more information on supported join operations.

A `projectItem` in `SELECT` can be a reference to a column, or aggregation expression using the supported aggregation functions listed in [Operators and Functions](#operators-and-functions). A `booleanExpression` is the same but must resolve to a `boolean` value.

**NOTE**: As of `v3.x.x`, a `projectItem` in `SELECT` must be unique, duplicated `projectItem` is no longer allowed and will throw an `SqlGremlinError`. Please raise an issue in the repository for feature implementation if this is impacting your usability. 

To order by a value, it must be part of the `SELECT` list. Group by and order by using column aliases is currently not supported.

Type Conversion is not supported, and thus `CAST` is not supported. Set operations `UNION`, `INTERSECT` and `EXCEPT` are not supported. Grouping operations using `CUBE`, `ROLLUP` or `GROUPING SETS` are not supported. Ordering using `NULLS FIRST` and `NULLS LAST` or by referencing column ordinals is not supported.

## Identifiers

Identifiers are the names of tables, columns, and column aliases in an SQL query.

Quoting is optional but unquoted identifiers must start with a letter and can only contain letters, digits, and underscores. Quoted identifiers start and end with double quotes. They may contain virtually any character. To include a double quote in an identifier, use another double quote to escape it. The maximum identifier length, quoted or unquoted, is 128 characters.

Identifier matching is case-sensitive and identifiers that match a reserved SQL keyword must be quoted or use fully qualified names.

## Joins

The driver only support `INNER JOIN` on two vertices that are connected by an edge. When looking at vertices you will see `<edge_label>_IN_ID` or `<edge_label>_OUT_ID`. Basic literal comparisons are supported in `WHERE` and `HAVING` clauses in conjunction with the `INNER JOIN` clause.

Foreign keys are not generated and not exposed in JDBC metadata at this time.

## Data Types

The driver recognizes the following SQL data types:

- `BOOLEAN` - Boolean literals must be `TRUE` or `FALSE`
- `TINYINT`
- `SMALLINT`
- `INTEGER` or `INT`
- `BIGINT`
- `DECIMAL`
- `REAL` or `FLOAT`
- `CHAR` and `VARCHAR` - String literal must be enclosed in single-quotes.
- `DATE`

## Operators and Functions

Supported operators are listed below. Arithmetic, string, conditional and date operators and functions are currently not support.

### Comparison Operators

- `value1 <op> value2` where `op` is one of : `=`, `<>`, `<`, `>`, `<=` or `>=`
- `NOT` in conjunction with a comparison operator is not currently supported

### Logical Operators

- `boolean1` OR `boolean2`
- `boolean1` AND `boolean2`
- NOT `boolean2` is supported only when used with a boolean valued column in `WHERE`/`HAVING` filters
- NOT `boolean2` is supported with expressions and boolean values in `SELECT` clause

### Aggregate Functions

- `AVG(numeric)`
- `COUNT(*)`
- `MAX(value)`
- `MIN(value)`
- `SUM(numeric)`

Currently, `COUNT( [DISTINCT] numeric)`, `COUNT( [DISTINCT] value)`, and `SUM( [DISTINCT] numeric)` are not supported.

# Schema Collection

Property graph databases are schemaless, however SQL requires that the connected database has schema. Because of this, sql-gremlin collects what is effectively the schema of the graph. This is done by sampling the graph, collecting all vertex and edge labels.

The vertex and edge labels become the tables in the graph. The properties of the vertices and edges become the columns in the graph. In addition to the properties of the vertices and edges, connections between vertices and edges are collected and become columns.

This means that a vertex with an edge labelled as *MY\_EDGE* that goes into it will have `MY_EDGE_IN_ID` as a column, and a vertex with an edge labelled as *MY\_OTHER\_EDGE* that goes out of it will have `MY_OTHER_EDGE_OUT_ID` as a column.

`MY_EDGE` will have the vertex is goes into as `<vertex_label>_IN_ID` as a column and `MY_OTHER_VERTEX` will have the vertex it goes out of as `<vertex_label>_OUT_ID` as a column.

Edges and columns also have their own id as a column with the name `<label>_ID`.

# Additional Limitations

Currently, JDBC driver supports [Tableau Data Extracts (TDE)](https://www.tableau.com/about/blog/2014/7/understanding-tableau-data-extracts-part1) and has limitations which may prevent or significantly limit functionality when using Live Connection in Tableau.

The driver does not support `Convert to Custom SQL` in Tableau, due to Tableau’s use of embedded SQL. If one wishes to issue SQL commands against a live server, [DbVisualizer](https://github.com/aws/amazon-neptune-jdbc-driver/blob/develop/markdown/bi-tools/DbVisualizer.md) does provide such functionality.

# Acknowledgements

Special thanks goes to the [Apache TinkerPop](http://tinkerpop.apache.org/) and [Apache Calcite](https://calcite.apache.org/) teams. The depth and breadth of both of these projects is truly astounding. Also, thanks to Daniel Kuppitz. His work on [SPARQL-Gremlin](https://github.com/dkuppitz/sparql-gremlin) served as a model and inspiration for SQL-Gremlin.
