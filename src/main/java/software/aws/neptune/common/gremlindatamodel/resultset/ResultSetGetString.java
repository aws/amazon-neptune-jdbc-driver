/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune.common.gremlindatamodel.resultset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.jdbc.utilities.SqlError;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * Base ResultSet for String types.
 */
public abstract class ResultSetGetString extends GenericResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResultSetGetString.class);
    private final List<String> columns;
    private final List<Map<String, String>> constantReturns;
    private boolean wasNull = false;

    /**
     * ResultSetGetString constructor, initializes super class.
     *
     * @param statement       Statement Object.
     * @param columns         Columns for result.
     * @param rowCount        Row count for result.
     * @param constantReturns Map of return values for given keys.
     */
    public ResultSetGetString(final Statement statement,
                              final List<String> columns,
                              final int rowCount,
                              final List<Map<String, String>> constantReturns) {
        super(statement, columns, rowCount);
        this.columns = columns;
        this.constantReturns = constantReturns;
    }

    @Override
    protected Object getConvertedValue(final int columnIndex) throws SQLException {
        verifyOpen();
        final int index = getRowIndex();
        if ((index >= constantReturns.size()) || (index < 0)
                || ((columnIndex > columns.size()) || (columnIndex <= 0))) {
            throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
        }
        final String key = columns.get(columnIndex - 1);
        if (constantReturns.get(index).containsKey(key)) {
            final Object value = constantReturns.get(index).get(key);
            wasNull = value == null;
            return value;
        } else {
            throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        return this.wasNull;
    }
}
