/*
 * Copyright <2020> Amazon.com, final Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, final Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, final WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, final either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.neptune.opencypher.resultset;

import org.neo4j.driver.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.SqlError;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class OpenCypherResultSetGetString extends OpenCypherResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCypherResultSetGetString.class);
    /**
     * TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     */
    private final List<Record> rows;
    private final List<String> columns;
    private final Map<String, String> constantReturns;

    /**
     * OpenCypherResultSetGetString constructor, initializes super class.
     *
     * @param statement       Statement Object.
     * @param rows            List of ordered rows.
     * @param columns         List of ordered columns.
     * @param constantReturns Map of return values for given keys.
     */
    public OpenCypherResultSetGetString(final Statement statement, final List<Record> rows, final List<String> columns,
                                        final Map<String, String> constantReturns) {
        super(statement, null, null, rows, columns);
        this.rows = rows;
        this.columns = columns;
        this.constantReturns = constantReturns;
    }

    @Override
    protected ResultSetMetaData getOpenCypherMetadata() throws SQLException {
        return new OpenCypherResultSetMetadataStringTypes(columns, rows);
    }

    @Override
    protected Object getConvertedValue(final int columnIndex) throws SQLException {
        verifyOpen();
        if (columnIndex <= columns.size() && columnIndex > 0) {
            final String key = columns.get(columnIndex - 1);
            if (constantReturns.containsKey(key)) {
                return constantReturns.get(key);
            }
        }
        throw SqlError
                .createSQLFeatureNotSupportedException(LOGGER, SqlError.INVALID_COLUMN_INDEX, columnIndex, columns.size());
    }
}
