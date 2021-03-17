# JDBC Driver for Amazon Neptune

This driver provides JDBC connectivity for the Amazon Neptune service using OpenCypher queries.

## Connection Requirements

To connect to Amazon Neptune using the JDBC driver, the Neptune instance must be available through an SSH tunnel, load balancer, or the JDBC driver must be deployed in an EC2 instance. 

## Specifications

This driver is compatible with JDBC 4.2 and requires a minimum of Java 8.

## Using the Driver

To use the JDBC driver, please see the [usage instructions](./USAGE.md).

## Troubleshooting

To troubleshoot or debug issues with the JDBC driver, please see the [troubleshooting instructions](./TROUBLESHOOTING.md).

## Contributing

Because the JDBC driver is available as open source, contribution from the community is encouraged. If you are interested in improving performance, adding new features, or fixing bugs, please see our [contributing guidlines](./CONTRIBUTING.md).

## Building from source

If you wish to contribute, you will need to build the driver. The requirements to build the driver are very simple, you only need a Java 8 compiler and runtime environment and you can build and run the driver. This library depends on the neptune-export library, which depends on the gremlin-client library. So before building this library, build the gremlin-client, then the neptune-export library, which are both included in this repository.

## Testing

The project is setup to do continuous unit testing whenever pull requests are generated, merged, or code is checked in. Integration tests can also be executed when major changes are made, but this will require coordination with a properly integrated server.

## Security issue notifications

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.

## Licensing

See the [LICENSE](./LICENSE) file for our project's licensing. We will ask you to confirm the licensing of your contribution.

## Copyright

Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
