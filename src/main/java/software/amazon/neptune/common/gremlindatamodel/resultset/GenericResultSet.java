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

package software.amazon.neptune.common.gremlindatamodel.resultset;

import org.neo4j.driver.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;
import software.amazon.neptune.opencypher.OpenCypherTypeMapping;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public abstract class GenericResultSet extends software.amazon.jdbc.ResultSet implements java.sql.ResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(GenericResultSet.class);
    private boolean wasNull = false;

    /**
     * OpenCypherResultSet constructor, initializes super class.
     *
     * @param statement     Statement Object.
     * @param columns       Columns for result.
     * @param rowCount      Row count for result.
     */
    public GenericResultSet(final java.sql.Statement statement, final List<String> columns, final int rowCount) {
        super(statement, columns, rowCount);
    }

    @Override
    protected void doClose() {
    }

    @Override
    protected int getDriverFetchSize() {
        // Do we want to update this or statement?
        return 0;
    }

    @Override
    protected void setDriverFetchSize(final int rows) {
        // Do we want to update this or statement?
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    private Value getValue(final int columnIndex) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLException(
                LOGGER,
                SqlState.DATA_EXCEPTION,
                SqlError.UNSUPPORTED_RESULT_SET_TYPE);
    }

    @Override
    public Object getObject(final int columnIndex, final Map<String, Class<?>> map) throws SQLException {
        LOGGER.trace("Getting column {} as an Object using provided Map.", columnIndex);
        final Value value = getValue(columnIndex);
        return getObject(columnIndex, map.get(OpenCypherTypeMapping.BOLT_TO_JDBC_TYPE_MAP.get(value.type()).name()));
    }
}
