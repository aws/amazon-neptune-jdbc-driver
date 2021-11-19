# Using SQL with Amazon Neptune JDBC Driver

This driver supports using SQL, which it will translate into Gremlin for executing statements against Neptune. For additional information about SQL and the type of SQL queries this driver supports, see the [sql-gremlin documentation](../sql-gremlin/README.asciidoc).

### Creating a connection

The connection string for SQL connections follows the following form:

`jdbc:neptune:sqlgremlin://[host];[port=portValue];[propertyKey1=value1];[propertyKey2=value2]..;[propertyKeyN=valueN]`

**Note: SQL configures the port as a property and not as a part of the connection string. If a port is not specified it defaults to 8182.**

The following properties are available for SQL:

| Property Key             | Description                                                  | Accepted Value(s)                                            | Default value                                                |
| ------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| logLevel                 | Log level for application.                                   | In order of least logging to most logging: `OFF`, `FATAL`, `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE`, `ALL`. | `INFO`                                                       |
| authScheme               | Authentication mechanism to use.                             | `NONE` (no auth), `IAMSigV4` (IAM / SIGV4 logging).          | If `IAMSigV4` is selected, the user must have AWS SIGV4 credentials properly set up in their environment, including a region. See [environment setup for IAM authentication on Neptune](https://docs.aws.amazon.com/neptune/latest/userguide/iam-auth-connecting-gremlin-java.html) for more information. |
| scanType                 | To scan all nodes or only the first node when creating database schema. | `ALL` (schema creation scans all nodes), `FIRST` (schema creation scans the first node only). |`ALL` |
| connectionTimeout        | Amount of time to wait for initial connection in _milliseconds_. | Integer values.                                              | `5000`                                                       |
| connectionRetryCount     | Number of times to retry if establishing initial connection fails. | Integer values.                                              | `3`                                                          |
| sshUser                  | The username for the internal SSH tunnel. If provided, options `sshHost` and `sshPrivateKeyFile` must also be provided, otherwise this option is ignored. | String values. |`NONE` |
| sshHost                  | The host name for the internal SSH tunnel. Optionally the SSH tunnel port number can be provided using the syntax `<ssh-host>:<port>`. The default port is `22`. If provided, options `sshUser` and `sshPrivateKeyFile` must also be provided, otherwise this option is ignored.  | String values. |`NONE` |
| sshPrivateKeyFile        | The path to the private key file for the internal SSH tunnel. If the path starts with the tilde character (`~`), it will be replaced with the user's home directory. If provided, options `sshUser` and `sshHost` must also be provided, otherwise this option is ignored.  | String values. |`NONE` |
| sshPrivateKeyPassphrase  | If the SSH tunnel private key file, `sshPrivateKeyFile`, is passphrase protected, provide the passphrase using this option. If provided, options `sshUser`, `sshHost` and `sshPrivateKeyFile` must also be provided, otherwise this option is ignored.  | String values. |`NONE` |
| sshStrictHostKeyChecking | If true, the 'known_hosts' file is checked to ensure the target host is trusted when creating the internal SSH tunnel. If false, the target host is not checked. Disabling this option is less secure as it can lead to a ["man-in-the-middle" attack](https://en.wikipedia.org/wiki/Man-in-the-middle_attack). If provided, options `sshUser`, `sshHost` and `sshPrivateKeyFile` must also be provided, otherwise this option is ignored. | Boolean values. |`true` |
| sshKnownHostsFile        | The path to the 'known_hosts' file used for checking the target host for the SSH tunnel when option `sshStrictHostKeyChecking` is `true`. The 'known_hosts' file can be populated using the `ssh-keyscan` [tool](maintain_known_hosts.md). If provided, options `sshUser`, `sshHost` and `sshPrivateKeyFile` must also be provided, otherwise this option is ignored. | String values. | `~/.ssh/known_hosts` |

#### Important note:
The above properties are configurations that are shared across all query languages in the driver.

*For all the Gremlin-specific properties take a look at their* [documentation](https://tinkerpop.apache.org/javadocs/current/full/org/apache/tinkerpop/gremlin/driver/Cluster.Builder.html).

Each Builder method listed in the documentation is accepted as a property. For example, "**[port](https://tinkerpop.apache.org/javadocs/current/full/org/apache/tinkerpop/gremlin/driver/Cluster.Builder.html#port-int-)**(int port)" means that as a part of the connection string it may be configured as a property or added as a Key-Value Pair in Properties. See below for examples.

#### Establishing Connection Manually
 
Amazon Neptune Database is a VPC only service, and you need to establish an SSH tunnel before connecting to the database outside of the VPC (e.g. from the local machine).
To establish an SSH tunnel, you need an EC2 instance deployed in the same VPC as Neptune Database. More information on how to configure SSH Tunnel is described in [Connecting to Neptune](/markdown/setup/configuration.md#using-an-ssh-tunnel-to-connect-to-amazon-neptune).

1. Establish Port Forwarding through SSH Tunnel
2. Add Neptune Database hostname in `hosts` (see [Adding SSH Tunnel Lookup](/markdown/setup/configuration.md#adding-a-ssh-tunnel-lookup) for more details how to add configuration in Windows and macOS).
3. Enter Connection String. As an example in [DBVisualizer](https://www.dbvis.com/) enter *Database URL* `jdbc:neptune:sqlgremlin://<hostname>;port=<port>;authScheme=NONE`. Default *port* is `8182`, however you can configure port forwarding to any available port on your machine. If you are using default port and using IAM authentication you do not need to supply additional parameters.
4. Now you can connect to your Amazon Neptune Database instance.


#### No authentication using string only

```java
import java.sql.*;

class Example {
    static final String CONNECTION_STRING = "jdbc:neptune:sqlgremlin://example.neptune.amazonaws.com;port=8182;authScheme=None";
    
    public static void main(String[] args) throws SQLException {
        Connection connection = DriverManager.getConnection(CONNECTION_STRING);
        connection.close();
    }
}
```

#### No authentication using Properties

Instead of having properties set in the connection string, you can also set Properties in order to get a connection.

```java
import java.sql.*;
import java.util.Properties;

class Example {
    static final String CONNECTION_STRING = "jdbc:neptune:sqlgremlin://example.neptune.amazonaws.com";
    
    public static void main(String[] args) throws SQLException {
        Properties properties = new Properties();
        properties.put("port", 8182);
        properties.put("authScheme", "None");
        
        Connection connection = DriverManager.getConnection(CONNECTION_STRING, properties);
        connection.close();
    }
}
```

#### IAM authentication

IAM is the standard way to access Neptune under an authorized account. Note that SSL is required for IAM authentication.

```java
import java.sql.*;

class Example {
    static final String CONNECTION_STRING = "jdbc:neptune:sqlgremlin://example.neptune.amazonaws.com;port=8182;authScheme=IAMSigV4;enableSsl=true;";
    
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
    static final String CONNECTION_STRING = "jdbc:neptune:sqlgremlin://example.neptune.amazonaws.com";
    
    public static void main(String[] args) throws SQLException {
        Properties properties = new Properties();
        properties.put("port", 8182);
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
    static final String CONNECTION_STRING = "jdbc:neptune:sqlgremlin://example.neptune.amazonaws.com;port=8182;authScheme=None";
    
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
    static final String CONNECTION_STRING = "jdbc:neptune:sqlgremlin://example.neptune.amazonaws.com;port=8182;authScheme=None";
    
    public static void main(String[] args) throws SQLException {
        String query = "SELECT * FROM country";
        try (
            // Create a connection
            Connection connection = DriverManager.getConnection(CONNECTION_STRING);
            // Create a statement
            Statement statement = connection.createStatement();
            // Execute a query
            ResultSet results = statement.executeQuery(query)
        ) {
            while (results.next()) {
                // Get the country name (DESC) from the results and print them
                String countryName = results.getString("DESC");
                System.out.println(countryName);
            }
        }
    }
}
```

