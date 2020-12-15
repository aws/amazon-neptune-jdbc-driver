package software.amazon.neptune;

import com.google.common.collect.ImmutableMap;
import software.amazon.jdbc.Driver;
import software.amazon.neptune.opencypher.OpenCypherConnection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NeptuneDriver extends Driver implements java.sql.Driver {
    private static final Pattern JDBC_PATTERN = Pattern.compile("jdbc:neptune:(\\w+)://(.*)");
    private static final Pattern PROP_PATTERN = Pattern.compile("(\\w+)=(\\w+)");
    private final Map<String, Class<?>> connectionMap = ImmutableMap.of("opencypher", OpenCypherConnection.class);

    @Override
    public boolean acceptsURL(final String url) throws SQLException {
        try {
            return connectionMap.containsKey(getLanguage(url));
        } catch (final SQLException ignored) {
        }
        return false;
    }

    @Override
    public java.sql.Connection connect(final String url, final Properties info) throws SQLException {
        try {
            final String language = getLanguage(url);
            if (!connectionMap.containsKey(language)) {
                return null;
            }
            final String propertyString = getPropertyString(url);
            final Properties properties = parsePropertyString(propertyString);
            properties.putAll(info);
            return (java.sql.Connection)connectionMap.get(language).getConstructor(Properties.class).newInstance(properties);
        } catch (final Exception e) {
            return null;
        }
    }

    private String getLanguage(final String url) throws SQLException {
        final Matcher matcher = JDBC_PATTERN.matcher(url);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        // TODO proper exception.
        throw new SQLException("Unsupported url " + url);
    }

    private String getPropertyString(final String url) throws SQLException {
        final Matcher matcher = JDBC_PATTERN.matcher(url);
        if (matcher.matches()) {
            return matcher.group(2);
        }
        // TODO proper exception.
        throw new SQLException("Unsupported property string.");
    }

    private Properties parsePropertyString(final String propertyString) {
        final Properties properties = new Properties();
        if (propertyString.isEmpty()) {
            return properties;
        }

        final String[] propertyArray = propertyString.split(";");
        if (propertyArray.length == 0) {
            return properties;
        } else if (!propertyArray[0].trim().isEmpty()) {
            properties.setProperty(NeptuneConstants.ENDPOINT, propertyArray[0].trim());
        }
        for (int i = 1; i < propertyArray.length; i++) {
            final Matcher matcher = PROP_PATTERN.matcher(propertyArray[i]);
            if (matcher.matches()) {
                properties.setProperty(matcher.group(1), matcher.group(2));
            }
        }
        return properties;
    }
}
