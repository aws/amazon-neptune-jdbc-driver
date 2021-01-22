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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility that converts log level coming as a connection string parameter into log4j.Level.
 */
public class LoggingLevel {
    public static final Level DEFAULT_LEVEL = Level.INFO;
    private static final Pattern KEY_PATTERN = Pattern.compile("logLevel", Pattern.CASE_INSENSITIVE);
    private static final Pattern ANY_VALUE_PATTERN = Pattern.compile("FATAL|ERROR|WARNINFO|DEBUG|TRACE", Pattern.CASE_INSENSITIVE);
    private static final Pattern FATAL_PATTERN = Pattern.compile("FATAL", Pattern.CASE_INSENSITIVE);
    private static final Pattern ERROR_PATTERN = Pattern.compile("ERROR", Pattern.CASE_INSENSITIVE);
    private static final Pattern WARN_PATTERN = Pattern.compile("WARN", Pattern.CASE_INSENSITIVE);
    private static final Pattern INFO_PATTERN = Pattern.compile("INFO", Pattern.CASE_INSENSITIVE);
    private static final Pattern DEBUG_PATTERN = Pattern.compile("DEBUG", Pattern.CASE_INSENSITIVE);
    private static final Pattern TRACE_PATTERN = Pattern.compile("TRACE", Pattern.CASE_INSENSITIVE);

    /**
     * Matches key/value property to check whether it represents Log Level settings.
     * @param key Property key as a case-insensitive string.
     * @param value Property value as a case-insensitive string.
     * @return True if the key/value property represents the Log Level setting, otherwise false.
     */
    public static Boolean matches(final String key, final String value) {
        final Matcher kayMatcher = KEY_PATTERN.matcher(key);
        final Matcher valueMatcher = ANY_VALUE_PATTERN.matcher(value);
        return (kayMatcher.matches() && valueMatcher.matches());
    }

    /**
     * Converts log level from a string to log4j.Level.
     * @param value Log level value as a case-insensitive string.
     * @return Log level as log4j.Level enum value.
     */
    public static Level getLevel(final String value) {
        Matcher matcher = FATAL_PATTERN.matcher(value);
        if (matcher.matches()) {
            return Level.FATAL;
        }
        matcher = ERROR_PATTERN.matcher(value);
        if (matcher.matches()) {
            return Level.ERROR;
        }
        matcher = WARN_PATTERN.matcher(value);
        if (matcher.matches()) {
            return Level.WARN;
        }
        matcher = INFO_PATTERN.matcher(value);
        if (matcher.matches()) {
            return Level.INFO;
        }
        matcher = DEBUG_PATTERN.matcher(value);
        if (matcher.matches()) {
            return Level.DEBUG;
        }
        matcher = TRACE_PATTERN.matcher(value);
        if (matcher.matches()) {
            return Level.TRACE;
        }
        return DEFAULT_LEVEL;
    }
}

