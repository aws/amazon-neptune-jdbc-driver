### Tableau Desktop

Before proceeding, ensure you have [configured your environment](../setup/configuration.md).

Download the latest version of [Tableau Desktop](https://www.tableau.com/products/desktop) to use the Driver in Tableau.

**For Mac you must launch your BI tool through terminal to have the environment variables loaded.**

#### Adding the Amazon Neptune JDBC Driver

1. [Download](../setup/configuration.md#neptune-jdbc-driver) the Neptune JDBC driver `JAR` file and copy it to one of these directories according to your operating system:
   - **_Windows_**: `C:\Program Files\Tableau\Drivers`
   - **_Mac_**: `~/Library/Tableau/Drivers`

2. [Download](../setup/configuration.md#neptune-jdbc-driver) the Neptune Tableau connector (a `TACO` file) and copy it to your `My Tableau Repository/Connectors`
   directory.
   - **_Windows_**: `C:\Users\[user]\Documents\My Tableau Repository\Connectors`
   - **_Mac_**: `/Users/[user]/Documents/My Tableau Repository/Connectors`

   For more information, consult the [Tableau documentation](https://tableau.github.io/connector-plugin-sdk/docs/run-taco).

#### Launching Tableau and Opening the Amazon Neptune Connector

1. Launch the Tableau Desktop application.

2. Navigate to **Connect > To A Server > More**. **Amazon Neptune by AWS** should be listed under **Installed Connectors**. Select it.

#### Connecting to Amazon Neptune Using Tableau - External SSH Tunnel

1. If connecting from outside the Neptune cluster's VPC, ensure you have followed the [configuration instructions](../configuration.md).

2. Enter the parameters. **Neptune Endpoint**, **Port**, **Use IAM Authentication**, and **Require SSL**
   are required, while the others are optional. Descriptions for each parameter can be found in
   the [SQL JDBC documentation](../sql.md).

![Tableau login dialog general tab](../images/tableau-sql-gremlin.png)

3. Click the **Sign In** button.
