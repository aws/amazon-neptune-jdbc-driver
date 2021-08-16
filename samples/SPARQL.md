# Using SPARQL with Amazon Neptune JDBC Driver

This driver supports using SPARQL for executing statements against Neptune.

### Creating a connection

The example connection string used in the code snippets below follow the rules specified in [the usage document](../USAGE.md). See that document for a more in-depth explanation of how to properly configure yours.

**Note: SPARQL configures the port as a property and not as a part of the connection string. If a port is not specified it defaults to 8182. There must also be a specification for the `queryEndpoint` property to be `sparql` and the connection string for SPARQL also must include a `https://` prefix as shown below.**

#### No authentication using string only

```java
import java.sql.*;

class Example {
    static final String CONNECTION_STRING = "jdbc:neptune:sparql://https://example.neptune.amazonaws.com;port=8182;queryEndpoint=sparql;authScheme=None;useEncryption=false;";
    
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
    static final String CONNECTION_STRING = "jdbc:neptune:sparql://https://example.neptune.amazonaws.com";
    
    public static void main(String[] args) throws SQLException {
        Properties properties = new Properties();
        properties.put("port", 8182);
        properties.put("queryEndpoint", "sparql");
        properties.put("authScheme", "None");
        properties.put("useEncryption", false);
        
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
    static final String CONNECTION_STRING = "jdbc:neptune:sparql://https://example.neptune.amazonaws.com;port=8182;queryEndpoint=sparql;authScheme=IAMSigV4;enableSsl=true;";
    
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
        properties.put("authScheme", "AWS_SIGv4");
        properties.put("useEncryption", true);
        
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
    static final String CONNECTION_STRING = "jdbc:neptune:sparql://https://example.neptune.amazonaws.com;port=8182;queryEndpoint=sparql;authScheme=None;useEncryption=false;";
    
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

