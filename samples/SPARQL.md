# Using SPARQL with Amazon Neptune JDBC Driver

This driver supports using SPARQL for executing statements against Neptune.

### Creating a connection

The connection string for SPARQL connections follows the following form:

`jdbc:neptune:sparql://https://[host];[port=portValue];[;propertyKey1=value1];[propertyKey2=value2]..;[propertyKeyN=valueN]`

**Note: SPARQL configures the port as a propertyKey and not as a part of the connection string. If a port is not specified it defaults to 8182. There must also be a specification for the `queryEndpoint` property to be `sparql` and the connection string for SPARQL also must include a `https://` prefix as shown below.**

The following properties are available for SPARQL:

| Property Key             | Description                                                  | Accepted Value(s)                                            | Default value                                                |
| ------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| logLevel                 | Log level for application.                                   | In order of least logging to most logging: `OFF`, `FATAL`, `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE`, `ALL`. | `INFO`                                                       |
| authScheme               | Authentication mechanism to use.                             | `NONE` (no auth), `IAMSigV4` (IAM / SIGV4 logging).          | If `IAMSigV4` is selected, the user must have AWS SIGV4 credentials properly set up in their environment, including a region. See [environment setup for IAM authentication on Neptune](https://docs.aws.amazon.com/neptune/latest/userguide/iam-auth-connecting-gremlin-java.html) for more information. |
| connectionTimeout        | Amount of time to wait for initial connection in _milliseconds_. | Integer values.                                              | `5000`                                                       |
| connectionRetryCount     | Number of times to retry if establishing initial connection fails. | Integer values.                                              | `3`                                                          |
| port                     | The port used for connection.                                | Integer values.                                              | `8182`                                                       |
| queryEndpoint            | The query endpoint to hit.                                   | Currently only `sparql`.                                     | `""`                                                         |
| region                   | The AWS endpoint region to connect to.                       | Valid AWS regions such as, but not limited to, `us-east-1`, `us-west-1`. | Default value is whatever is configured in the user's AWS SIG4 credentials. |
| dataset                  | The name of the dataset when connecting to a FusekiServer (not applicable to Neptune servers). | String values.                                               | `""`                                                         |
| acceptHeaderQuery        | The HTTP `Accept:` header used to when making a SPARQL Protocol query if no query type specific setting available. | String values.                                               | `"application/rdf+xml, application/n-triples, text/turtle, text/plain, application/n-quads, text/x-nquads, text/turtle, application/trig, text/n3, application/ld+json, application/trix, application/x-binary-rdf, application/sparql-results+json, application/sparql-results+xml;q=0.9, text/tab-separated-values;q=0.7, text/csv;q=0.5, application/json;q=0.2, application/xml;q=0.2, /*;q=0.1"` |
| acceptHeaderAskQuery     | The HTTP `Accept:` header used to when making a SPARQL Protocol ASK query. | String values.                                               | `None`                                                         |
| acceptHeaderSelectQuery  | The HTTP `Accept:` header used to when making a SPARQL Protocol SELECT query. | String values.                                               | `None`                                                         |
| parseCheckSparql         | The flag for whether to check SPARQL queries and SPARQL updates provided as a string. | Boolean values.                                              | `None`                                                         |
| acceptHeaderDataset      | The HTTP `Accept:` header used to fetch RDF datasets using HTTP GET operations. | String values.                                               | `None`                                                         |
| httpClient               | The `HttpClient` for the connection to be built.             | `httpClient` values.                                         | `None`                                                         |
| httpContext              | The `HttpContext` for the connection to tbe built            | `httpContext` values.                                        | `None`                                                         |
| sshUser                  | The username for the internal SSH tunnel. If provided, options `sshHost` and `sshPrivateKeyFile` must also be provided, otherwise this option is ignored. | String values. |`NONE` |
| sshHost                  | The host name for the internal SSH tunnel. Optionally the SSH tunnel port number can be provided using the syntax `<ssh-host>:<port>`. The default port is `22`. If provided, options `sshUser` and `sshPrivateKeyFile` must also be provided, otherwise this option is ignored.  | String values. |`NONE` |
| sshPrivateKeyFile        | The path to the private key file for the internal SSH tunnel. If the path starts with the tilde character (`~`), it will be replaced with the user's home directory. If provided, options `sshUser` and `sshHost` must also be provided, otherwise this option is ignored.  | String values. |`NONE` |
| sshPrivateKeyPassphrase  | If the SSH tunnel private key file, `sshPrivateKeyFile`, is passphrase protected, provide the passphrase using this option. If provided, options `sshUser`, `sshHost` and `sshPrivateKeyFile` must also be provided, otherwise this option is ignored.  | String values. |`NONE` |
| sshStrictHostKeyChecking | If true, the 'known_hosts' file is checked to ensure the target host is trusted when creating the internal SSH tunnel. If false, the target host is not checked. Disabling this option is less secure as it can lead to a ["man-in-the-middle" attack](https://en.wikipedia.org/wiki/Man-in-the-middle_attack). If provided, options `sshUser`, `sshHost` and `sshPrivateKeyFile` must also be provided, otherwise this option is ignored. | Boolean values. |`true` |
| sshKnownHostsFile        | The path to the 'known_hosts' file used for checking the target host for the SSH tunnel when option `sshStrictHostKeyChecking` is `true`. The 'known_hosts' file can be populated using the `ssh-keyscan` [tool](maintain_known_hosts.md). If provided, options `sshUser`, `sshHost` and `sshPrivateKeyFile` must also be provided, otherwise this option is ignored. | String values. | `~/.ssh/known_hosts` |

#### No authentication using string only

```java
import java.sql.*;

class Example {
    static final String CONNECTION_STRING = "jdbc:neptune:sparql://https://example.neptune.amazonaws.com;port=8182;queryEndpoint=sparql;authScheme=None";
    
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
    static final String CONNECTION_STRING = "jdbc:neptune:sparql://https://example.neptune.amazonaws.com";
    
    public static void main(String[] args) throws SQLException {
        Properties properties = new Properties();
        properties.put("port", 8182);
        properties.put("queryEndpoint", "sparql");
        properties.put("authScheme", "None");
        
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
    static final String CONNECTION_STRING = "jdbc:neptune:sparql://https://example.neptune.amazonaws.com;port=8182;queryEndpoint=sparql;authScheme=IAMSigV4";
    
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
    static final String CONNECTION_STRING = "jdbc:neptune:sparql://https://example.neptune.amazonaws.com";
    
    public static void main(String[] args) throws SQLException {
        Properties properties = new Properties();
        properties.put("port", 8182);
        properties.put("queryEndpoint", "sparql");
        properties.put("authScheme", "IAMSigV4");
        
        Connection connection = DriverManager.getConnection(CONNECTION_STRING, properties);
        connection.close();
    }
}
```

### Querying the database

#### Listing the table names

MetaData is not currently supported for SPARQL.

#### Executing a statement

```java
import java.sql.*;

class Example {
    static final String CONNECTION_STRING = "jdbc:neptune:sparql://https://example.neptune.amazonaws.com;port=8182;queryEndpoint=sparql;authScheme=None";
    
    public static void main(String[] args) throws SQLException {
        // Prefix for data found at https://github.com/aws/graph-notebook
        String query = 
            "PREFIX prop:  <http://kelvinlawrence.net/air-routes/datatypeProperty/>\n" +
            "PREFIX class: <http://kelvinlawrence.net/air-routes/class/>\n" +
            "\n" +
            "SELECT ?desc\n" +
            "WHERE {\n" +
            "    ?s ?p ?desc .\n" +
            "    ?s a class:Country .\n" +
            "    ?s prop:desc ?desc\n" +
            "}";
        try (
            // Create a connection
            Connection connection = DriverManager.getConnection(CONNECTION_STRING);
            // Create a statement
            Statement statement = connection.createStatement();
            // Execute a query
            ResultSet results = statement.executeQuery(query)
        ) {
            while (results.next()) {
                // Get the country name (desc) from the results and print them
                String countryName = results.getString("desc");
                System.out.println(countryName);
            }
        }
    }
}
```

