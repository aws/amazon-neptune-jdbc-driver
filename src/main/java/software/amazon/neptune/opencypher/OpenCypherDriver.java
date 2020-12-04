package software.amazon.neptune.opencypher;

import software.amazon.jdbc.Driver;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class OpenCypherDriver extends Driver implements java.sql.Driver {
    // TODO: Implement files.
    @Override
    public Connection connect(final String url, final Properties info) throws SQLException {
        return null;
    }

    @Override
    public boolean acceptsURL(final String url) throws SQLException {
        return false;
    }
}
