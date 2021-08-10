# Using openCypher with Amazon Neptune JDBC Driver

This driver supports using openCypher for executing statements against Neptune.

### Creating a connection

The example connection string used in the code snippets below follow the rules specified in [the usage document](../USAGE.md). See that document for a more in-depth explanation of how to properly configure yours.

**Note: For openCypher, the port must be specified as a part of the connection string and `useEncryption` must be set to `true` (defaults to true). The host must also be prefixed with `//bolt:` to specify usage of the Bolt connector.**

#### No authentication using string only

```java
import java.sql.*;

class Example {
    static final String CONNECTION_STRING = "jdbc:neptune:opencypher://bolt://example.neptune.amazonaws.com:8182;authScheme=None;useEncryption=true;";
    
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

IAM is the standard way to access Neptune under an authorized account. Note that SSL is required for IAM authentication.

```java
import java.sql.*;

class Example {
    static final String CONNECTION_STRING = "jdbc:neptune:opencypher://bolt://example.neptune.amazonaws.com:8182;authScheme=IAMSigV4;enableSsl=true;";
    
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
        properties.put("authScheme", "AWS_SIGv4");
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
    static final String CONNECTION_STRING = "jdbc:neptune:opencypher://bolt://example.neptune.amazonaws.com:8182;authScheme=None;useEncryption=true;";
    
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
    static final String CONNECTION_STRING = "jdbc:neptune:opencypher://bolt://example.neptune.amazonaws.com:8182;authScheme=None;useEncryption=true;";
    
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



