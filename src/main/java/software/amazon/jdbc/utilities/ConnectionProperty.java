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
 */

package software.amazon.jdbc.utilities;

import org.apache.log4j.Level;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.regex.Pattern;

/**
 * Enum containing all supported connection properties.
 */
public enum ConnectionProperty {
    APPLICATION_NAME("ApplicationName",
            "The name of the application currently utilizing the connection.",
            ""),
    AWS_CREDENTIALS_PROVIDER_CLASS("AwsCredentialsProviderClass",
            "The AWSCredentialsProvider class that user wants to use.",
            ""),
    CUSTOM_CREDENTIALS_FILE_PATH("CustomCredentialsFilePath",
            "The AWS credentials file that user wants to use.",
            ""),
    ACCESS_KEY_ID("AccessKeyId",
            "The AWS user access key id.",
            ""),
    ENDPOINT("endpoint",
            "Endpoint to connect to.",
            "",
            Pattern.compile("Endpoint", Pattern.CASE_INSENSITIVE)),
    SECRET_ACCESS_KEY("SecretAccessKey",
            "The AWS user secret access key.",
            ""),
    SESSION_TOKEN("SessionToken",
            "The database's region.",
            ""),
    LOG_LEVEL("LogLevel",
            "Log level.",
            Level.INFO,
            Pattern.compile("logLevel", Pattern.CASE_INSENSITIVE)),
    CONNECTION_TIMEOUT("ConnectionTimeout",
            "Connection timeout.",
            5000,
            Pattern.compile("ConnectionTimeout", Pattern.CASE_INSENSITIVE)),
    CONNECTION_RETRY_COUNT("ConnectionRetryCount",
            "Connection retry count.",
            3,
            Pattern.compile("ConnectionRetryCount", Pattern.CASE_INSENSITIVE));

    static final EnumSet<ConnectionProperty> SENSITIVE_PROPERTIES = EnumSet
            .of(ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN);

    private final String connectionProperty;
    private final String description;
    private final Object defaultValue;
    private final Pattern regexPattern;

    /**
     * ConnectionProperty constructor.
     *
     * @param connectionProperty String representing the connection property.
     * @param description Description of the property.
     * @param defaultValue Default value of the property.
     */
    ConnectionProperty(
            final String connectionProperty,
            final String description,
            final Object defaultValue) {
        this(connectionProperty, description, defaultValue, null);
    }

    /**
     * ConnectionProperty constructor.
     *
     * @param connectionProperty String representing the connection property.
     * @param description Description of the property.
     * @param defaultValue Default value of the property.
     * @param regexPattern Optional regex pattern used to extract value from the connection string.
     */
    ConnectionProperty(
            final String connectionProperty,
            final String description,
            final Object defaultValue,
            final Pattern regexPattern) {
        this.connectionProperty = connectionProperty;
        this.description = description;
        this.defaultValue = defaultValue;
        this.regexPattern = regexPattern;
    }

    /**
     * Gets connection property.
     *
     * @return the connection property.
     */
    public String getConnectionProperty() {
        return connectionProperty;
    }

    /**
     * Gets description.
     *
     * @return the description.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Gets default value.
     *
     * @return the default value.
     */
    public Object getDefaultValue() {
        return defaultValue;
    }

    /**
     * Gets regex pattern.
     *
     * @return the regex pattern .
     */
    public Pattern getPattern() {
        return regexPattern;
    }

    /**
     * Check if the property is supported by the driver.
     *
     * @param name The name of the property.
     * @return {@code true} if property is supported; {@code false} otherwise.
     */
    public static boolean isSupportedProperty(final String name) {
        return Arrays
                .stream(ConnectionProperty.values())
                .anyMatch(value -> value.getConnectionProperty().equals(name));
    }
}
