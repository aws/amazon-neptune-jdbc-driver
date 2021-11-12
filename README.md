# JDBC Driver for Amazon Neptune

This driver provides read-only JDBC connectivity for the Amazon Neptune service using SQL, Gremlin, openCypher and SPARQL queries.

## Using the Driver

The driver comes packed in a single jar file. To use the driver, place the jar file in the classpath of the application which is going to use it.

[//]: # (TODO AN-694 - Uncomment this: Alternatively, if using the driver with a Maven/Gradle application, the jar can be used to install the driver via their respective commands.)

For the initial public preview release, the driver will be available for download on GitHub along with the driver's .jar file and .taco file.

To use the Driver in BI tools, please refer to the documentation below. 

To connect to Amazon Neptune using the JDBC driver, the Neptune instance must be available through an SSH tunnel, load balancer, or the JDBC driver must be deployed in an EC2 instance.

**SSH Tunnel and host file must be configured before using the drive to connect to Neptune, please see [SSH configuration](markdown/setup/configuration.md).**

### Specifications

This driver is compatible with JDBC 4.2 and requires a minimum of Java 8.

### Connection URL and Settings

To set up a connection, the driver requires a JDBC connection URL. The connection URL is generally of the form:

```
jdbc:neptune:[connectionType]://[host];[propertyKey1=value1];[propertyKey2=value2]..;[propertyKeyN=valueN]
```

Specific requirements for the string can be found [below](#graph-query-language-support) in the specific query language documentation.

### Connecting using the DriverManager Interface

If the jar is in the application's classpath, no other configuration is required. The driver can be connected to using the JDBC DriverManager by connecting using an Amazon Neptune connection string.

Below is an example where Neptune is accessible through the endpoint neptune-example.com on port 8182.

```
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

void example() {
    String url = "jdbc:neptune:opencypher://bolt://neptune-example:8182";

    Connection connection = DriverManager.getConnection(url);
    Statement statement = connection.createStatement();
    
    connection.close();
}
```

Refer to the connection string options in the specific query language documentation below for more information about configuring the connection.

For more example applications, see the [sample applications](./src/test/java/sample/applications).

## Graph Query Language Support

### SQL
The driver supports a subset of SQL-92 and some common extensions. 

To connection to Amazon Neptune using SQL, please see the [SQL connection configurations](markdown/sql.md) for details about connection string configurations. 

#### For information on the limitations of the SQL query support please see the [SQL specifications](sql-gremlin/README.asciidoc).

### Gremlin

Gremlin is a graph traversal language supported by Neptune. To issue Gremlin queries to Neptune though the driver, please see
[Gremlin connection configurations](markdown/gremlin.md).

### openCypher

openCypher is an open query language for property graph database supported by Neptune. To issue openCypher queries to Neptune though the driver, please see
[openCypher connection configurations](markdown/opencypher.md).

### SPARQL

SPARQL is an RDF query language supported by Neptune. To issue SPARQL queries to Neptune though the driver, please see
[SPARQL connection configurations](markdown/sparql.md).

## Driver Setup in BI Applications

To learn how to set up the driver in various BI tools, instructions are outlined here for:
* [Tableau Desktop](markdown/bi-tools/tableau.md)

## Troubleshooting

To troubleshoot or debug issues with the JDBC driver, please see the [troubleshooting instructions](markdown/troubleshooting.md).

## Contributing

Because the JDBC driver is available as open source, contribution from the community is encouraged. If you are interested in improving performance, adding new features, or fixing bugs, please see our [contributing guidelines](./CONTRIBUTING.md).

## Building from source

If you wish to contribute, you will need to build the driver. The requirements to build the driver are very simple, you only need a Java 8 compiler and runtime environment and you can build and run the driver. This library depends on the neptune-export library, which depends on the gremlin-client library. So before building this library, build the gremlin-client, then the neptune-export library, which are both included in this repository.

## Security issue notifications

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.

## Licensing

See the [LICENSE](./LICENSE) file for our project's licensing. We will ask you to confirm the licensing of your contribution.

## Copyright

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
