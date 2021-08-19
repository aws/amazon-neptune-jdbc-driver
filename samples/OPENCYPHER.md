# Using openCypher with Amazon Neptune JDBC Driver

This driver supports using openCypher for executing statements against Neptune.

### Creating a connection

The connection string for openCypher connections follows the following form:

`jdbc:neptune:opencypher://bolt://[host]:[port];[propertyKey1=value1];[propertyKey2=value2]..;[propertyKeyN=valueN]`

**Note: For openCypher, the port must be specified as a part of the connection string not as a propertyKey. Property `useEncryption` must be set to `true` (defaults to true). The host must also be prefixed with `//bolt:` to specify usage of the Bolt connector.**

The following properties are available for openCypher:

| Property Key         | Description                                                  | Accepted Value(s)                                            | Default value                                                |
| -------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| logLevel             | Log level for application.                                   | In order of least logging to most logging: `OFF`, `FATAL`, `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE`, `ALL`. | `INFO`                                                       |
| authScheme           | Authentication mechanism to use.                             | `NONE` (no auth), `IAMSigV4` (IAM / SIGV4 logging).          | If `IAMSigV4` is selected, the user must have AWS SIGV4 credentials properly set up in their environment, including a region. See [environment setup for IAM authentication on Neptune](https://docs.aws.amazon.com/neptune/latest/userguide/iam-auth-connecting-gremlin-java.html) for more information. |
| connectionTimeout    | Amount of time to wait for initial connection in _milliseconds_. | Integer values.                                              | `5000`                                                       |
| connectionRetryCount | Number of times to retry if establishing initial connection fails. | Integer values.                                              | `3`                                                          |
| connectionPoolSize   | The max size of the connection pool to establish with the cluster. | Integer values.                                              | `1000`                                                       |
| useEncryption        | Whether to establish the connection over _SSL/TLS_.          | `true` or `false`.                                           | Default value is `true`.                                     |
| region               | The AWS endpoint region to connect to.                       | Valid AWS regions such as, but not limited to, `us-east-1`, `us-west-1`. | Default value is whatever is configured in the user's AWS SIG4 credentials. |

#### No authentication using string only

```java
import java.sql.*;

class Example {
    static final String CONNECTION_STRING = "jdbc:neptune:opencypher://bolt://example.neptune.amazonaws.com:8182;authScheme=None;useEncryption=true";
    
    public static void main(String[] args) throws SQLException {
        Connection connection = DriverManager.getConnection(CONNECTION_STRING);
        connection.close();
    }
}
```

#### No authentication using Properties

Instead of having properties set in the connection string, you can also set Properties in order to get a connection. 

[The usage document](../USAGE.md) has a list of settings that can be set in the properties.

```java
import java.sql.*;
import java.util.Properties;

class Example {
    static final String CONNECTION_STRING = "jdbc:neptune:opencypher://bolt://example.neptune.amazonaws.com:8182";
    
    public static void main(String[] args) throws SQLException {
        Properties properties = new Properties();
        properties.put("authScheme", "None");
        properties.put("useEncryption", true);
        
        Connection connection = DriverManager.getConnection(CONNECTION_STRING, properties);
        connection.close();
    }
}
```

#### IAM authentication

IAM is the standard way to access Neptune under an authorized account.

```java
import java.sql.*;

class Example {
    static final String CONNECTION_STRING = "jdbc:neptune:opencypher://bolt://example.neptune.amazonaws.com:8182;authScheme=IAMSigV4;useEncryption=true";
    
    public static void main(String[] args) throws SQLException {
        Connection connection = DriverManager.getConnection(CONNECTION_STRING);
        connection.close();
    }
}
```

Like above, Properties can be used to set the settings to enable IAM authentication.

```java
import java.sql.*;
import java.util.Properties;

class Example {
    static final String CONNECTION_STRING = "jdbc:neptune:opencypher://bolt://example.neptune.amazonaws.com:8182";
    
    public static void main(String[] args) throws SQLException {
        Properties properties = new Properties();
        properties.put("authScheme", "IAMSigV4");
        properties.put("useEncryption", true);
        
        Connection connection = DriverManager.getConnection(CONNECTION_STRING, properties);
        connection.close();
    }
}
```

### Querying the database

#### Listing the table names

```java
import java.sql.*;

class Example {
    static final String CONNECTION_STRING = "jdbc:neptune:opencypher://bolt://example.neptune.amazonaws.com:8182;authScheme=None;useEncryption=true";
    
    public static void main(String[] args) throws SQLException {
        // Create a connection
        try (Connection connection = DriverManager.getConnection(CONNECTION_STRING)) {
            // Get the MetaData of the database
            DatabaseMetaData metaData = connection.getMetaData();
            // Get the tables from the MetaData
            try (ResultSet results = metaData.getTables(null, null, "%", null)) {
                while (results.next()) {
                    // Get the table name from the results and print them
                    String tableName = results.getString("TABLE_NAME");
                    System.out.println(tableName);
                }
            }
        }
    }
}
```

#### Executing a statement

```java
import java.sql.*;

class Example {
    static final String CONNECTION_STRING = "jdbc:neptune:opencypher://bolt://example.neptune.amazonaws.com:8182;authScheme=None;useEncryption=true";
    
    public static void main(String[] args) throws SQLException {
        String query = "MATCH (c:country) RETURN c.desc";
        try (
            // Create a connection
            Connection connection = DriverManager.getConnection(CONNECTION_STRING);
            // Create a statement
            Statement statement = connection.createStatement();
            // Execute a query
            ResultSet results = statement.executeQuery(query)
        ) {
            while (results.next()) {
                // Get the queried country name from the results and print them
                String countryName = results.getString(1);
                System.out.println(countryName);
            }
        }
    }
}
```

