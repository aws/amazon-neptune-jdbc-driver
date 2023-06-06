# Troubleshooting

## JDBC Documentation

Refer to the [JDBC documentation](https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/) for more information about how to use a JDBC driver.

## Common Encountered Issues

The sections below detail some commonly encountered errors and detail how they can be detected and fixed.

### Failed to Connect to Server

Use the `isValid` function of the JDBC `Connection` Object to check if the connection is valid. If the function returns false (meaning the connection is invalid), check that the endpoint being connected to is correct and that you are in the VPC of your Neptune cluster (or have a valid SSH tunnel to the cluster).

Make sure your port and url are both correct in your endpoint. The format should be `jdbc:neptune:opencypher://<url>:port`.

### Connection string issues

If you get `No suitable driver found for <connection string>` from the `DriverManager.getConnection` call, then you have an issue with your connection string. This is likely due to an issue in the beginning of the connection string, double check that your connection string starts with `jdbc:neptune:opencypher://`. 

### Maven Issues

If you use the regular thin JDBC driver (e.g. [amazon-neptune-jdbc-driver-3.0.0.jar](https://repo1.maven.org/maven2/software/amazon/neptune/amazon-neptune-jdbc-driver/3.0.0/amazon-neptune-jdbc-driver-3.0.0.jar)) as a dependency for your Maven project, you may encounter errors with transitive dependencies. If you notice exceptions like `java.lang.ClassNotFoundException: com.fasterxml.jackson.core.util.JacksonFeature` when running your application, then you probably have a version conflict with the dependencies. You must add an exclusion for that particular artifact and include the correct version. Since the JDBC driver is built using Gradle, you can emulate how Gradle resolves dependencies and include the highest version from the dependency graph.

The example below shows how to exclude Jackson Library in case of conflicts and include the version that is required in the project.

```
<dependencies>
    <dependency>
        <groupId>software.amazon.neptune</groupId>
        <artifactId>amazon-neptune-jdbc-driver</artifactId>
        <version>3.0.2</version>
        <exclusions>
            <exclusion>
                <groupId>com.fasterxml.jackson.core</groupId>
                <artifactId>jackson-core</artifactId>
            </exclusion>
            <exclusion>
                <groupId>com.fasterxml.jackson.core</groupId>
                <artifactId>jackson-annotations</artifactId>
            </exclusion>
        </exclusions>
    </dependency>
    <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-core</artifactId>
        <version>2.12.3</version>
    </dependency>
    <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-annotations</artifactId>
        <version>2.12.3</version>
    </dependency>
</dependencies>
```

## Tips to Gather Useful Information

By adding a LogLevel to your connection string as `jdbc:neptune:opencypher://<url>:<port>;logLevel=trace` or add `properties.put("logLevel", "trace")` in your input properties to increase the amount of logging that is output.