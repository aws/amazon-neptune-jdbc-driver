package software.amazon.neptune.opencypher;

import javax.sql.PooledConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;


/**
 * OpenCypher implementation of DataSource.
 */
public class OpenCypherDataSource extends software.amazon.jdbc.DataSource implements javax.sql.DataSource, javax.sql.ConnectionPoolDataSource {
    private final Properties properties;

    /**
     * OpenCypherDataSource constructor, initializes super class.
     * @param properties Properties Object.
     */
    OpenCypherDataSource(final Properties properties) {
        super();
        this.properties = (Properties) properties.clone();
    }

    @Override
    public java.sql.Connection getConnection() throws SQLException {
        return new OpenCypherConnection(properties);
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        // TODO: Add some auth logic.
        return null;
    }

    @Override
    public PooledConnection getPooledConnection() throws SQLException {
        return new OpenCypherPooledConnection(getConnection());
    }

    @Override
    public PooledConnection getPooledConnection(final String user, final String password) throws SQLException {
        return new OpenCypherPooledConnection(getConnection(user, password));
    }

    // TODO: Implement
    @Override
    public void setLoginTimeout(final int seconds) throws SQLException {

    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }
}
