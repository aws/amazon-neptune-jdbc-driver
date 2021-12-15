/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.twilmes.sql.gremlin.adapter.util;

import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 * Enum representing the possible error messages and lookup facilities for localization.
 */
public enum SqlGremlinError {
    IDENTIFIER_INDEX_OUT_OF_BOUNDS,
    IDENTIFIER_LIST_EMPTY,
    OPERANDS_MORE_THAN_TWO,
    OPERANDS_EMPTY,
    IDENTIFIER_SIZE_INCORRECT,
    ID_BASED_APPEND,
    SCHEMA_NOT_SET,
    SQL_SELECT_ONLY,
    PARSE_ERROR,
    EDGE_LABEL_END_MISMATCH,
    EDGE_EXPECTED,
    TABLE_DOES_NOT_EXIST,
    UNKNOWN_NODE,
    UNKNOWN_NODE_GETFROM,
    UNKNOWN_NODE_ISTABLE,
    UNKNOWN_OPERATOR,
    TYPE_MISMATCH,
    ERROR_TABLE,
    UNEXPECTED_OPERAND,
    UNEXPECTED_OPERAND_INDEX,
    OPERANDS_EXPECTED_TWO_SQL_AS,
    FAILED_GET_NAME_ACTUAL,
    FAILED_GET_NAME_RENAME,
    UNEXPECTED_NODE_GREMLINSQLBASICCALL,
    UNEXPECTED_NODE_GREMLINSQLAGGFUNCTION,
    COLUMN_RENAME_UNDETERMINED,
    COLUMN_ACTUAL_NAME_UNDETERMINED,
    FAILED_RENAME_GREMLINSQLAGGOPERATOR,
    AGGREGATE_NOT_SUPPORTED,
    COLUMN_NOT_FOUND,
    NO_ORDER,
    ONLY_NOT_PREFIX_SUPPORTED,
    BINARY_AND_PREFIX_OPERAND_COUNT,
    UNKNOWN_NODE_SELECTLIST,
    JOIN_TABLE_COUNT,
    INNER_JOIN_ONLY,
    JOIN_ON_ONLY,
    LEFT_RIGHT_CONDITION_OPERANDS,
    LEFT_RIGHT_AS_OPERATOR,
    JOIN_EDGELESS_VERTICES,
    CANNOT_GROUP_EDGES,
    CANNOT_GROUP_TABLE,
    CANNOT_GROUP_COLUMN,
    JOIN_HAVING_UNSUPPORTED,
    SINGLE_SELECT_MULTI_RETURN,
    SELECT_NO_LIST,
    UNEXPECTED_FROM_FORMAT,
    COLUMN_RENAME_LIST_EMPTY,
    NO_TRAVERSAL_TABLE,
    CANNOT_ORDER_COLUMN_LITERAL,
    CANNOT_ORDER_BY,
    ORDER_BY_ORDINAL_VALUE,
    WHERE_NOT_ONLY_BOOLEAN,
    WHERE_UNSUPPORTED_PREFIX,
    WHERE_BASIC_LITERALS,
    UNEXPECTED_JOIN_NODES,
    NO_JOIN_COLUMN,
    NOT_LOGICAL_FILTER,
    OFFSET_NOT_SUPPORTED,
    UNSUPPORTED_LITERAL_EXPRESSION;

    private static final ResourceBundle RESOURCE;

    static {
        Locale.setDefault(Locale.ENGLISH);
        RESOURCE = ResourceBundle.getBundle("error-messages");
    }

    /**
     * Looks up the resource bundle string corresponding to the key, and formats it with the provided
     * arguments.
     *
     * @param key        Resource key for bundle provided to constructor.
     * @param formatArgs Any additional arguments to format the resource string with.
     * @return resource String, formatted with formatArgs.
     */
    public static String getMessage(final SqlGremlinError key, final Object... formatArgs) {
        return String.format(RESOURCE.getString(key.name()), formatArgs);
    }

    /**
     * Helper method for creating an appropriate SQLException.
     *
     * @param key           Key for the error message.
     * @param logger        Logger for logging the error message.
     * @param cause         The underlying cause for the SQLException.
     * @param formatArgs    Any additional arguments to format the error message with.
     * @return SQLException
     */
    public static SQLException create(final SqlGremlinError key, final Logger logger, final Throwable cause,
                                      final boolean isNotSupported, final Object... formatArgs) {
        final String message = getMessage(key, formatArgs);
        final SQLException exception;

        if (cause == null) {
            exception = isNotSupported ? new SQLNotSupportedException(message) : new SQLException(message);
            if (logger != null) {
                logger.error(message);
            }
        } else {
            exception = isNotSupported ? new SQLNotSupportedException(message, cause) : new SQLException(message, cause);
            if (logger != null) {
                logger.error(message, cause);
            }
        }

        return exception;
    }

    /**
     * Helper method for creating an appropriate SQLException.
     *
     * @param key           Key for the error message.
     * @param logger        Logger for logging the error message.
     * @param formatArgs    Any additional arguments to format the error message with.
     * @return SQLException
     */
     public static SQLException create(final SqlGremlinError key, final Logger logger, final Object... formatArgs) {
        return create(key, logger, null, false, formatArgs);
     }

    /**
     * Helper method for creating an appropriate SQLException.
     *
     * @param key           Key for the error message.
     * @param cause         The underlying cause for the SQLException.
     * @param formatArgs    Any additional arguments to format the error message with.
     * @return SQLException
     */
     public static SQLException create(final SqlGremlinError key, final Throwable cause, final Object... formatArgs) {
        return create(key, null, cause, false, formatArgs);
     }

    /**
     * Helper method for creating an appropriate SQLException.
     *
     * @param key           Key for the error message.
     * @param formatArgs    Any additional arguments to format the error message with.
     * @return SQLException
     */
     public static SQLException create(final SqlGremlinError key, final Object... formatArgs) {
        return create(key, null, null, false, formatArgs);
     }

    /**
     * Helper method for creating an appropriate SQLNotSupportedException.
     *
     * @param key           Key for the error message.
     * @param logger        Logger for logging the error message.
     * @param cause         The underlying cause for the SQLNotSupportedException.
     * @param formatArgs    Any additional arguments to format the error message with.
     * @return SQLException
     */
    public static SQLNotSupportedException createNotSupported(
            final SqlGremlinError key,
            final Logger logger,
            final Throwable cause,
            final Object... formatArgs) {
        return (SQLNotSupportedException)create(key, logger, cause, true, formatArgs);
    }

    /**
     * Helper method for creating an appropriate SQLNotSupportedException.
     *
     * @param key           Key for the error message.
     * @param logger        Logger for logging the error message.
     * @param formatArgs    Any additional arguments to format the error message with.
     * @return SQLException
     */
    public static SQLNotSupportedException createNotSupported(final SqlGremlinError key, final Logger logger,
                                                  final Object... formatArgs) {
        return createNotSupported(key, logger, null, formatArgs);
    }

    /**
     * Helper method for creating an appropriate SQLNotSupportedException.
     *
     * @param key           Key for the error message.
     * @param cause         The underlying cause for the SQLNotSupportedException.
     * @param formatArgs    Any additional arguments to format the error message with.
     * @return SQLException
     */
    public static SQLNotSupportedException createNotSupported(final SqlGremlinError key, final Throwable cause,
                                                  final Object... formatArgs) {
        return createNotSupported(key, null, cause, formatArgs);
    }

    /**
     * Helper method for creating an appropriate SQLNotSupportedException.
     *
     * @param key           Key for the error message.
     * @param formatArgs    Any additional arguments to format the error message with.
     * @return SQLException
     */
    public static SQLNotSupportedException createNotSupported(final SqlGremlinError key, final Object... formatArgs) {
        return createNotSupported(key, null, null, formatArgs);
    }
}
