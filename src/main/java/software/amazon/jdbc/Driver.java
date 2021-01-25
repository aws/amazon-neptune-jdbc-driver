/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.jdbc;

import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
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
    static final int DRIVER_MAJOR_VERSION;
    static final int DRIVER_MINOR_VERSION;
    static final String DRIVER_VERSION;
    static final String APP_NAME_SUFFIX;
    static final String APPLICATION_NAME;
    private static final Pattern KEY_VALUE_PATTERN = Pattern.compile("(\\w+)=(\\w+)");
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Driver.class);

    static {
        APPLICATION_NAME = getApplicationName();
        // TODO: Get driver version, suffix
        DRIVER_MAJOR_VERSION = 1;
        DRIVER_MINOR_VERSION = 1;
        APP_NAME_SUFFIX = "TODO";
        DRIVER_VERSION = "0.0.0";
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

    // TODO: Fix functions below.
    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    protected String getLanguage(final String url, final Pattern jdbcPattern) throws SQLException {
        final Matcher matcher = jdbcPattern.matcher(url);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        // TODO proper exception.
        throw new SQLException("Unsupported url " + url);
    }

    protected String getPropertyString(final String url, final Pattern jdbcPattern) throws SQLException {
        final Matcher matcher = jdbcPattern.matcher(url);
        if (matcher.matches()) {
            return matcher.group(2);
        }
        // TODO proper exception.
        throw new SQLException("Unsupported property string.");
    }

    protected Properties parsePropertyString(final String propertyString, final String endpoint) {
        final Properties properties = new Properties();
        if (propertyString.isEmpty()) {
            return properties;
        }

        final String[] propertyArray = propertyString.split(";");
        if (propertyArray.length == 0) {
            return properties;
        } else if (!propertyArray[0].trim().isEmpty()) {
            properties.setProperty(endpoint, propertyArray[0].trim());
        }
        for (int i = 1; i < propertyArray.length; i++) {
            final Matcher propMatcher = KEY_VALUE_PATTERN.matcher(propertyArray[i]);
            if (propMatcher.matches()) {
                properties.setProperty(propMatcher.group(1), propMatcher.group(2));
            }
        }
        return properties;
    }
}
