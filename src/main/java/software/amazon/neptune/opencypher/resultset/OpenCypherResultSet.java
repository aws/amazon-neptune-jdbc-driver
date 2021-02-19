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

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;
import software.amazon.neptune.opencypher.OpenCypherTypeMapping;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OpenCypherResultSet extends software.amazon.jdbc.ResultSet implements java.sql.ResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCypherResultSet.class);
    private final List<String> columns;
    private final List<Record> rows;
    private final Result result;
    private final Session session;
    private boolean wasNull = false;

    /**
     * OpenCypherResultSet constructor, initializes super class.
     *
     * @param statement     Statement Object.
     * @param resultSetInfo ResultSetInfoWithRows Object.
     */
    public OpenCypherResultSet(final java.sql.Statement statement, final ResultSetInfoWithRows resultSetInfo) {
        super(statement, resultSetInfo.getColumns(), resultSetInfo.getRows().size());
        this.session = resultSetInfo.getSession();
        this.result = resultSetInfo.getResult();
        this.columns = resultSetInfo.getColumns();
        this.rows = resultSetInfo.getRows();
    }

    /**
     * OpenCypherResultSet constructor, initializes super class.
     *
     * @param statement Statement Object.
     * @param resultSetInfo ResultSetInfoWithoutRows Object.
     */
    public OpenCypherResultSet(final java.sql.Statement statement, final ResultSetInfoWithoutRows resultSetInfo) {
        super(statement, resultSetInfo.getColumns(), resultSetInfo.getRowCount());
        this.session = resultSetInfo.getSession();
        this.result = resultSetInfo.getResult();
        this.columns = resultSetInfo.getColumns();
        this.rows = null;
    }

    @Override
    protected void doClose() throws SQLException {
        if (result != null) {
            result.consume();
        }
        if (session != null) {
            session.close();
        }
    }

    @Override
    protected int getDriverFetchSize() throws SQLException {
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

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return getOpenCypherMetadata();
    }

    protected ResultSetMetaData getOpenCypherMetadata() throws SQLException {
        final List<Type> rowTypes = new ArrayList<>();
        if (rows == null) {
            for (int i = 0; i < columns.size(); i++) {
                rowTypes.add(InternalTypeSystem.TYPE_SYSTEM.STRING());
            }
        } else {
            // TODO: Loop through records and do some sort of type promotion.
            final Record record = rows.get(0);
            for (int i = 0; i < columns.size(); i++) {
                rowTypes.add(record.get(i).type());
            }
        }
        return new OpenCypherResultSetMetadata(columns, rowTypes);
    }

    protected Object getConvertedValue(final int columnIndex) throws SQLException {
        final Value value = getValue(columnIndex);
        final OpenCypherTypeMapping.Converter<?> converter = getConverter(value);
        return converter.convert(value);
    }

    private Value getValue(final int columnIndex) throws SQLException {
        verifyOpen();
        if (rows == null) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.UNSUPPORTED_RESULT_SET_TYPE);
        }
        validateRowColumn(columnIndex);
        final Value value = rows.get(getRowIndex()).get(columnIndex - 1);
        wasNull = value.isNull();
        return value;
    }

    protected OpenCypherTypeMapping.Converter<?> getConverter(final Value value) {
        return OpenCypherTypeMapping.BOLT_TO_JAVA_TRANSFORM_MAP.get(value.type());
    }

    @Override
    public Object getObject(final int columnIndex, final Map<String, Class<?>> map) throws SQLException {
        LOGGER.trace("Getting column {} as an Object using provided Map.", columnIndex);
        final Value value = getValue(columnIndex);
        return getObject(columnIndex, map.get(OpenCypherTypeMapping.BOLT_TO_JDBC_TYPE_MAP.get(value.type()).name()));
    }

    @AllArgsConstructor
    @Getter
    public static class ResultSetInfoWithRows {
        private final Session session;
        private final Result result;
        private final List<Record> rows;
        private final List<String> columns;
    }

    @AllArgsConstructor
    @Getter
    public static class ResultSetInfoWithoutRows {
        private final Session session;
        private final Result result;
        private final int rowCount;
        private final List<String> columns;
    }
}
