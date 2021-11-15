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

## Tips to Gather Useful Information

By adding a LogLevel to your connection string as `jdbc:neptune:opencypher://<url>:<port>;logLevel=trace` or add `properties.put("logLevel", "trace")` in your input properties to increase the amount of logging that is output.