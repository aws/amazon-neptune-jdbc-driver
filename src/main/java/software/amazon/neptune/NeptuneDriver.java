package software.amazon.neptune;

import com.google.common.collect.ImmutableMap;
import software.amazon.jdbc.Driver;
import software.amazon.neptune.opencypher.OpenCypherConnection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class NeptuneDriver extends Driver implements java.sql.Driver {
    private static final Pattern JDBC_PATTERN = Pattern.compile("jdbc:neptune:(\\w+)://(.*)");

    private final Map<String, Class<?>> connectionMap = ImmutableMap.of("opencypher", OpenCypherConnection.class);

    @Override
    public boolean acceptsURL(final String url) throws SQLException {
        try {
            return connectionMap.containsKey(getLanguage(url, JDBC_PATTERN));
        } catch (final SQLException ignored) {
        }
        return false;
    }

    @Override
    public java.sql.Connection connect(final String url, final Properties info) throws SQLException {
        try {
            final String language = getLanguage(url, JDBC_PATTERN);
            if (!connectionMap.containsKey(language)) {
                return null;
            }
            final String propertyString = getPropertyString(url, JDBC_PATTERN);
            final Properties properties = parsePropertyString(propertyString);
            properties.putAll(info);
            return (java.sql.Connection)connectionMap.get(language).getConstructor(Properties.class).newInstance(properties);
        } catch (final Exception e) {
            return null;
        }
    }
}
