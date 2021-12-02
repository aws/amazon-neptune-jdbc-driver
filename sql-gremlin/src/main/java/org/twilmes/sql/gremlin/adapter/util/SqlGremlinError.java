package org.twilmes.sql.gremlin.adapter.util;

import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.ResourceBundle;

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
    BINARY_OPERAND_COUNT,
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
    NOT_LOGICAL_FILTER;

    private static final ResourceBundle RESOURCE = ResourceBundle.getBundle("error-messages");

    private static String getMessage(final SqlGremlinError key, final Object... formatArgs) {
        return String.format(RESOURCE.getString(key.name()), formatArgs);
    }

    public static SQLException get(final SqlGremlinError key, final Logger logger, final Exception cause,
                                   final Object... formatArgs) {
        final String message = getMessage(key, formatArgs);
        final SQLException exception;

        if (cause == null) {
            exception = new SQLException(message);
            if (logger != null) {
                logger.error(message);
            }
        } else {
            exception = new SQLException(message, cause);
            if (logger != null) {
                logger.error(message, cause);
            }
        }

        return exception;
    }

     public static SQLException get(final SqlGremlinError key, final Logger logger, final Object... formatArgs) {
        return get(key, logger, null, formatArgs);
     }

     public static SQLException get(final SqlGremlinError key, final Exception cause, final Object... formatArgs) {
        return get(key, null, cause, formatArgs);
     }

     public static SQLException get(final SqlGremlinError key, final Object... formatArgs) {
        return get(key, null, null, formatArgs);
     }
}
