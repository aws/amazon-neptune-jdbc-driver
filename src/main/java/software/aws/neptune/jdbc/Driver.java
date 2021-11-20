/*
 * Copyright <2021> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.aws.neptune.jdbc;

import org.slf4j.LoggerFactory;
import software.aws.neptune.jdbc.utilities.SqlError;
import software.aws.neptune.jdbc.utilities.SqlState;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Abstract implementation of Driver for JDBC Driver.
 */
public abstract class Driver implements java.sql.Driver {
    public static final int DRIVER_MAJOR_VERSION;
    public static final int DRIVER_MINOR_VERSION;
    public static final String DRIVER_VERSION;
    public static final String APP_NAME_SUFFIX;
    public static final String APPLICATION_NAME;
    private static final String PROPERTIES_PATH = "/project.properties";
    private static final String MAJOR_VERSION_KEY = "driver.major.version";
    private static final String MINOR_VERSION_KEY = "driver.minor.version";
    private static final String VERSION_KEY = "driver.full.version";
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Driver.class);

    static {
        APPLICATION_NAME = getApplicationName();
        // TODO: suffix
        APP_NAME_SUFFIX = "TODO";

        int majorVersion = 0;
        int minorVersion = 0;
        String version = "";
        try (InputStream input = Driver.class.getResourceAsStream(PROPERTIES_PATH)) {
            final Properties properties = new Properties();
            properties.load(input);
            majorVersion = Integer.parseInt(properties.getProperty(MAJOR_VERSION_KEY));
            minorVersion = Integer.parseInt(properties.getProperty(MINOR_VERSION_KEY));
            version = properties.getProperty(VERSION_KEY);
        } catch (IOException e) {
            LOGGER.error("Error loading driver version: ", e);
        }

        DRIVER_MAJOR_VERSION = majorVersion;
        DRIVER_MINOR_VERSION = minorVersion;
        DRIVER_VERSION = version;
    }

    /**
     * Get the name of the currently running application.
     *
     * @return the name of the currently running application.
     */
    private static String getApplicationName() {
        // What we do is get the process ID of the current process, then check the set of running processes and pick out
        // the one that matches the current process. From there we can grab the name of what is running the process.
        try {
            final String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
            final boolean isWindows = System.getProperty("os.name").startsWith("Windows");

            if (isWindows) {
                final Process process = Runtime.getRuntime()
                        .exec("tasklist /fi \"PID eq " + pid + "\" /fo csv /nh");
                try (final BufferedReader input = new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    final String line = input.readLine();
                    if (line != null) {
                        // Omit the surrounding quotes.
                        return line.substring(1, line.indexOf(",") - 1);
                    }
                }
            } else {
                final Process process = Runtime.getRuntime().exec("ps -eo pid,comm");
                try (final BufferedReader input = new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = input.readLine()) != null) {
                        line = line.trim();
                        if (line.startsWith(pid)) {
                            return line.substring(line.indexOf(" ") + 1);
                        }
                    }
                }
            }
        } catch (final Exception err) {
            // Eat the exception and fall through.
            LOGGER.info(
                    "An exception has occurred and ignored while retrieving the caller application name: "
                            + err.getLocalizedMessage());
        }

        return "Unknown";
    }

    @Override
    public java.sql.DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) throws SQLException {
        return new java.sql.DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return DRIVER_MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    protected String getLanguage(final String url, final Pattern jdbcPattern) throws SQLException {
        final Matcher matcher = jdbcPattern.matcher(url);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        throw SqlError.createSQLException(
                LOGGER,
                SqlState.CONNECTION_EXCEPTION,
                SqlError.UNSUPPORTED_LANGUAGE, url);
    }

    protected String getPropertyString(final String url, final Pattern jdbcPattern) throws SQLException {
        final Matcher matcher = jdbcPattern.matcher(url);
        if (matcher.matches()) {
            return matcher.group(2);
        }
        throw SqlError.createSQLException(
                LOGGER,
                SqlState.CONNECTION_EXCEPTION,
                SqlError.UNSUPPORTED_PROPERTIES_STRING, url);
    }

    protected Properties parsePropertyString(final String propertyString, final String firstPropertyKey) {
        final Properties properties = new Properties();
        if (propertyString.isEmpty()) {
            return properties;
        }

        final String[] propertyArray = propertyString.split(";");
        if (propertyArray.length == 0) {
            return properties;
        } else if (!propertyArray[0].trim().isEmpty()) {
            properties.setProperty(firstPropertyKey, propertyArray[0].trim());
        }
        for (int i = 1; i < propertyArray.length; i++) {
            if (propertyArray[i].contains("=")) {
                final String[] keyValue = propertyArray[i].split("=");
                if (keyValue.length != 2) {
                    LOGGER.warn("Encountered property that could not be parsed: " + propertyArray[i] + ".");
                } else {
                    properties.setProperty(keyValue[0], keyValue[1]);
                }
            } else {
                properties.setProperty(propertyArray[i], "");
            }
        }
        return properties;
    }
}
