### Tableau Desktop

Before proceeding, ensure you have [configured your environment](../setup/configuration.md).

Download the latest version of [Tableau Desktop](https://www.tableau.com/products/desktop) to use the Driver in Tableau.

#### Adding the Amazon Neptune JDBC Driver

1. [Download](https://github.com/aws/amazon-neptune-jdbc-driver/releases) the Neptune JDBC driver `JAR` file and copy it to one of these directories according to your operating system:
   - **_Windows_**: `C:\Program Files\Tableau\Drivers`
   - **_Mac_**: `~/Library/Tableau/Drivers`

2. [Download](https://github.com/aws/amazon-neptune-jdbc-driver/releases) the Neptune Tableau connector (a `TACO` file) and copy it to your `My Tableau Repository/Connectors`
   directory.
   
   - **_Windows_**: `C:\Users\[user]\Documents\My Tableau Repository\Connectors`
   - **_Mac_**: `/Users/[user]/Documents/My Tableau Repository/Connectors`
   For more information, consult the [Tableau documentation](https://tableau.github.io/connector-plugin-sdk/docs/run-taco).

#### Launching Tableau and Opening the Amazon Neptune Connector

1. Launch the Tableau Desktop application.

2. Navigate to **Connect > To A Server > More**. **Amazon Neptune by Amazon Web Services** should be listed under **Installed Connectors**. Select it. 

<p align="center">
<img alt="driver" src="../images/driver_selection.png" width="65%"/>
</p>

#### Connecting to Amazon Neptune Using Tableau - External SSH Tunnel

1. If connecting from outside the Neptune cluster's VPC, ensure you have followed the [configuration instructions](../setup/configuration.md) for your Neptune instance and local SSH tunnel.

2. Enter the connection parameters as shown below. **Neptune Endpoint**, **Port**, **Use IAM Authentication** (and **Service Region** if IAM is selected), and **Require SSL** are required. 

Example for connecting to an instance without IAM authentication:
<p align="center">
<img alt="non-auth" src="../images/non_auth.png" width="55%"/>
</p>
Example for connecting to an instance with IAM authentication. Note that service region is a required field:
<p align="center">
<img alt="iam-auth" src="../images/iam_auth.png" width="55%"/>
</p>

3. Additional connection options can be configured in the Advanced tab. Descriptions of each parameter can be found in the [SQL JDBC documentation](../sql.md). Please note that we have disabled all SSH tunnel options in the Tableau connector, due to potentially processing sensitive information. Please set up your SSH tunnel according to the [configuration instructions](../setup/configuration.md#using-an-ssh-tunnel-to-connect-to-amazon-neptune). 
<p align="center">
<img alt="add_opts" src="../images/add_opts.png" width="55%"/>
</p>

4. Click the **Sign In** button.
