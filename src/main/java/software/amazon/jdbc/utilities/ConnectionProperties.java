package software.amazon.jdbc.utilities;

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.Connection;

import java.util.Properties;

import static software.amazon.jdbc.utilities.ConnectionProperty.CONNECTION_RETRY_COUNT;
import static software.amazon.jdbc.utilities.ConnectionProperty.CONNECTION_TIMEOUT;
import static software.amazon.jdbc.utilities.ConnectionProperty.LOG_LEVEL;

/**
 * Class that manages connection properties.
 */
public class ConnectionProperties {
    private static final Logger LOGGER = LoggerFactory.getLogger(Connection.class);
    private final Properties connectionProperties;

    /**
     * ConnectionProperties constructor.
     * @param connectionProperties initial set of connection properties coming from connection string.
     */
    public ConnectionProperties(final Properties connectionProperties) {
        this.connectionProperties = ConnectionPropertiesUtility.extractValidProperties(connectionProperties);
    }

    /**
     * Gets all connection properties.
     * @return all connection properties.
     */
    public Properties getAll() {
        return connectionProperties;
    }

    /**
     * Clear content of the connectionProperties map.
     */
    public void clear() {
        connectionProperties.clear();
    }

    /**
     * Puts a Connection property into the map.
     * @param key Connection property key.
     * @param value Connection property value.
     */
    public void put(final Object key, final Object value) {
        connectionProperties.put(key, value);
    }

    /**
     * Gets connection property string value for the given key.
     * @return Connection property value for the given key.
     */
    public String get(final String key) {
        return connectionProperties.get(key).toString();
    }

    /**
     * Retrieves LogLevel connection property value.
     * @return LogLevel connection property value.
     */
    public Level getLogLevel() {
        if (connectionProperties.containsKey(LOG_LEVEL.getConnectionProperty())) {
            return (Level)connectionProperties.get(LOG_LEVEL.getConnectionProperty());
        }
        return (Level)LOG_LEVEL.getDefaultValue();
    }

    /**
     * Retrieves ConnectionTimeout connection property value.
     * @return ConnectionTimeout connection property value.
     */
    public int getConnectionTimeout() {
        if (connectionProperties.containsKey(CONNECTION_TIMEOUT.getConnectionProperty())) {
            return (int) connectionProperties.get(CONNECTION_TIMEOUT.getConnectionProperty());
        }
        return (int)CONNECTION_TIMEOUT.getDefaultValue();
    }

    /**
     * Retrieves ConnectionRetryCount connection property value.
     * @return ConnectionRetryCount connection property value.
     */
    public int getConnectionRetryCount() {
        if (connectionProperties.containsKey(CONNECTION_RETRY_COUNT.getConnectionProperty())) {
            return (int) connectionProperties.get(CONNECTION_RETRY_COUNT.getConnectionProperty());
        }
        return (int)CONNECTION_RETRY_COUNT.getDefaultValue();
    }
}
