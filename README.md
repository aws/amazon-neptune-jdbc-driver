# JDBC Driver for Amazon Neptune

This driver provides read-only JDBC connectivity for the Amazon Neptune service using SQL, Gremlin, openCypher and SPARQL queries.

## Using the Driver

The driver is available on [Maven Central](https://search.maven.org/search?q=g:software.amazon.neptune%20AND%20a:amazon-neptune-jdbc-driver).
In addition to the regular jar, a shadow/uber/fat jar is also published with the classifier `all`. 

To use the Driver in BI tools, please refer to the documentation below. 

To connect to Amazon Neptune using the JDBC driver, the Neptune instance must be available through an SSH tunnel, load balancer, or the JDBC driver must be deployed in an EC2 instance.

**SSH Tunnel and host file must be configured before using the drive to connect to Neptune, please see [SSH configuration](markdown/setup/configuration.md).**

### Specifications

This driver is compatible with JDBC 4.2 and requires a minimum of Java 8.

### Compatibility with AWS Neptune

| Engine Release           | Driver Version |
|--------------------------|----------------|
| < 1.1.1.0                | 1.1.0          |
| < 1.2.0.0 and >= 1.1.1.0 | 2.0.0+         |
| >= 1.2.0.0               | 3.0.0+         |

### Connection URL and Settings

To set up a connection, the driver requires a JDBC connection URL. The connection URL is generally of the form:

```
jdbc:neptune:[connectionType]://[host];[propertyKey1=value1];[propertyKey2=value2]..;[propertyKeyN=valueN]
```

A basic example of a connection string is:

```jdbc:neptune:sqlgremlin://neptune-example.com;port=8182```

Specific requirements for the string can be found [below](#graph-query-language-support) in the specific query language documentation.

### Connecting using the DriverManager Interface

If the jar is in the application's classpath, no other configuration is required. The driver can be connected to using the JDBC DriverManager by connecting using an Amazon Neptune connection string.

Below is an example where Neptune is accessible through the endpoint neptune-example.com on port 8182.

**Reminder: The Neptune endpoint is only accessible if a SSH tunnel is established to a EC2 instance in the same Amazon VPC as the Neptune cluster.**

In this example, the SSH tunnel would have been established by running something similar to the following in a shell:

```ssh -i "ec2Access.pem" -L 8182:neptune-example.com:8182 ubuntu@ec2-34-229-221-164.compute-1.amazonaws.com -N ```

The full documentation for how to establish the SSH tunnel can once again be found [here](markdown/setup/configuration.md).

```
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

void example() {
    String url = "jdbc:neptune:sqlgremlin://neptune-example.com;port=8182";

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
* [DbVisualizer](markdown/bi-tools/DbVisualizer.md)
* [DBeaver](markdown/bi-tools/DBeaver.md)

## Troubleshooting

To troubleshoot or debug issues with the JDBC driver, please see the [troubleshooting instructions](markdown/troubleshooting.md).

## Contributing

Because the JDBC driver is available as open source, contribution from the community is encouraged. If you are interested in improving performance, adding new features, or fixing bugs, please see our [contributing guidelines](./CONTRIBUTING.md).

## Building from Source

If you wish to contribute, you will need to build the driver. The requirements to build the driver are very simple, you only need a Java 8 compiler with a runtime environment and you can build and run the driver.

## Security Issue Notifications

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.

## Licensing

See the [LICENSE](./LICENSE) file for our project's licensing. We will ask you to confirm the licensing of your contribution.

## Copyright

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
