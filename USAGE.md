# Amazon Neptune JDBC Driver

This driver provides read-only JDBC connectivity for the Amazon Neptune service using OpenCypher queries, but does not block write queries. If write queries.

## Using the Driver

The driver comes packed in a single jar file. To use the driver, place the jar file in the classpath of the application which is going to use it.

### Connection URL and Settings

To setup a connection, the driver requires a JDBC connection URL. The connection URL is of the form:
```
    jdbc:neptune:[connectionType]://[host]:[port]?[propertyKey1=value1][;propertyKey2=value2]..[;propertyKeyN=valueN]
```

* connectionType

  The only supported option is `opencypher` in this release.

* host

  Hostname or IP address of the target cluster or load balancer.

* port

  Port number to connect to the cluster or load balancer on. Typically Neptune is communicated on through port `8182`.

* propertyKey=value

  The query string portion of the connection URL can contain desired connection settings in the form of one or more
  *propertyKey=value* pairs. The possible configuration properties are provided in the table below. The propertyKeys are not case-sensitive.

  Note that JDBC provides multiple APIs for specifying connection properties of which specifying them in the connection
  URL is just one. When directly coding with the driver you can choose any of the other options (refer sample
  code below). If you are setting up a connection via a tool, it is likely the tool will allow you to specify the
  connection URL with just the scheme, host, port and context-path components) while the the connection properties are provided separately.

  The configurable connection properties are:

  | Property Key  | Description | Accepted Value(s)    | Default value  |
  | ------------- |-------------| -----|---------|
  | logLevel | Log level for application. | In order least logging to most logging: `OFF`, `FATAL`, `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE`, `ALL` | `INFO` |
  | authScheme | Authentication mechanism to use. | `NONE` (no auth), `AWS_SIGV4` (IAM / SIGV4 logging) | If `AWS_SIGV4` is selected, the user must have AWS SIGV4 credentials properly set up in their environment, including a region. See [environment setup for IAM authentication on Neptune](https://docs.aws.amazon.com/neptune/latest/userguide/iam-auth-connecting-gremlin-java.html) for more information. |
  | useEncryption | Whether to establish the connection over _SSL/TLS_. | `true` or `false` | Default value is `true`. |
  | connectionTimeout | Amount of time to wait for initial connection in _milliseconds_. | Integer values. | `5000` |
  | connectionPoolSize | The max size of the connection pool to establish with the cluster. | Integer values. | `1000` |
  | connectionRetryCount | Number of times to retry if establishing initial connection fails. | Integer values. | `3` |


#### Connecting using the DriverManager Interface

If the jar is in the applications classpath, no other configuration is required. The driver can be connected to using the JDBC DriverManager by connecting using an Amazon Neptune connection string for OpenCypher.

Below is an example where Neptune is accessible through the endpoint neptune-example.com on port 8182.

```
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

void example() {
    String url = "jdbc:neptune:opencypher://neptune-example:8182";

    Connection connection = DriverManager.getConnection(url);
    Statement statement = connection.createStatement();
    
    connection.close();
}
```

Refer to the connection string options for more information about configuring the connection. For more example applications, see the [sample applications](./src/test/java/sample/applications).

### Querying the Graph

```
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

void example() {
    String url = "jdbc:neptune:opencypher://neptune-example:8182";

    Connection connection = DriverManager.getConnection(url);
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("RETURN null as x");
    
    connection.close();
}
```

## Download and Installation

TODO: Once this is deployed we can put instructions here.

## Copyright

Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
