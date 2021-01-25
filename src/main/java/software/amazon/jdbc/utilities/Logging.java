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

import com.google.common.collect.ImmutableMap;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility that converts log level coming as a connection string parameter into log4j.Level.
 */
public class Logging {
    public static final Level DEFAULT_LEVEL = Level.INFO;
    private static final Pattern KEY_PATTERN = Pattern.compile("logLevel", Pattern.CASE_INSENSITIVE);
    private static final Pattern ANY_VALUE_PATTERN = Pattern.compile("FATAL|ERROR|WARNINFO|DEBUG|TRACE", Pattern.CASE_INSENSITIVE);
    private static final Map<Pattern, Level> LOGGING_LEVELS = ImmutableMap.<Pattern, Level>builder()
        .put(Pattern.compile("FATAL", Pattern.CASE_INSENSITIVE), Level.FATAL)
        .put(Pattern.compile("ERROR", Pattern.CASE_INSENSITIVE), Level.ERROR)
        .put(Pattern.compile("WARN", Pattern.CASE_INSENSITIVE), Level.WARN)
        .put(Pattern.compile("INFO", Pattern.CASE_INSENSITIVE), Level.INFO)
        .put(Pattern.compile("DEBUG", Pattern.CASE_INSENSITIVE), Level.DEBUG)
        .put(Pattern.compile("TRACE", Pattern.CASE_INSENSITIVE), Level.TRACE)
        .build();

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
     * Converts Logging level from a string to the log4j.Level.
     * @param value Logging level as a case-insensitive string.
     * @return Logging level as a log4j.Level enum value.
     */
    public static Level convertToLevel(final String value) {
        for (Map.Entry<Pattern, Level> entry : LOGGING_LEVELS.entrySet()) {
            final Matcher matcher = (entry.getKey()).matcher(value);
            if (matcher.matches()) {
                return entry.getValue();
            }
        }
        return DEFAULT_LEVEL;
    }

    /**
     * Set logging level.
     * @param level Logging level as a log4j.Level enum value.
     */
    public static void setLevel(final Level level) {
        LogManager.getRootLogger().setLevel(level);
    }
}

