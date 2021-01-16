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

package software.amazon.neptune.opencypher;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.JavaToJdbcTypeConverter;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * OpenCypher implementation of ResultSet.
 */
public class OpenCypherResultSet extends software.amazon.jdbc.ResultSet implements java.sql.ResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCypherResultSet.class);
    private final Result result;
    private final List<String> columns;
    private final List<Record> rows;
    private final Session session;
    private boolean wasNull = false;
    private int rowIndex = -1;

    /**
     * OpenCypherResultSet constructor, initializes super class.
     *
     * @param statement Statement Object.
     * @param result    Result Object.
     * @param session   Session Object.
     */
    OpenCypherResultSet(final java.sql.Statement statement, final Result result, final Session session) {
        super(statement);
        this.session = session;
        this.result = result;
        this.rows = result.list();
        this.columns = result.keys();
    }

    @Override
    protected void doClose() {
        result.consume();
        session.close();
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
    protected int getRowIndex() {
        return this.rowIndex;
    }

    @Override
    protected int getRowCount() {
        return rows.size();
    }

    @Override
    public boolean next() throws SQLException {
        // Increment row index, if it exceeds capacity, set it to 1 after the last element.
        if (++this.rowIndex >= rows.size()) {
            this.rowIndex = rows.size();
        }
        return (this.rowIndex < rows.size());
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new OpenCypherResultSetMetadata(columns, rows);
    }

    @Override
    public int findColumn(final String columnLabel) throws SQLException {
        return columns.indexOf(columnLabel);
    }

    @Override
    public boolean getBoolean(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a Boolean.", columnIndex);
        final Value value = getValue(columnIndex);
        final OpenCypherTypeMapping.Converter<?> converter = getConverter(value);
        return JavaToJdbcTypeConverter.toBoolean(converter.convert(value));
    }

    @Override
    public byte getByte(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a Byte.", columnIndex);
        final Value value = getValue(columnIndex);
        final OpenCypherTypeMapping.Converter<?> converter = getConverter(value);
        return JavaToJdbcTypeConverter.toByte(converter.convert(value));
    }

    @Override
    public short getShort(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a Short.", columnIndex);
        final Value value = getValue(columnIndex);
        final OpenCypherTypeMapping.Converter<?> converter = getConverter(value);
        return JavaToJdbcTypeConverter.toShort(converter.convert(value));
    }

    @Override
    public int getInt(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a Integer.", columnIndex);
        final Value value = getValue(columnIndex);
        final OpenCypherTypeMapping.Converter<?> converter = getConverter(value);
        return JavaToJdbcTypeConverter.toInteger(converter.convert(value));
    }

    @Override
    public long getLong(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a Long.", columnIndex);
        final Value value = getValue(columnIndex);
        final OpenCypherTypeMapping.Converter<?> converter = getConverter(value);
        return JavaToJdbcTypeConverter.toLong(converter.convert(value));
    }

    @Override
    public float getFloat(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a Float.", columnIndex);
        final Value value = getValue(columnIndex);
        final OpenCypherTypeMapping.Converter<?> converter = getConverter(value);
        return JavaToJdbcTypeConverter.toFloat(converter.convert(value));
    }

    @Override
    public double getDouble(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a Double.", columnIndex);
        final Value value = getValue(columnIndex);
        final OpenCypherTypeMapping.Converter<?> converter = getConverter(value);
        return JavaToJdbcTypeConverter.toDouble(converter.convert(value));
    }

    @Override
    public String getString(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a String.", columnIndex);
        final Value value = getValue(columnIndex);
        final OpenCypherTypeMapping.Converter<?> converter = getConverter(value);
        return JavaToJdbcTypeConverter.toString(converter.convert(value));
    }

    @Override
    public byte[] getBytes(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a byte array.", columnIndex);
        final Value value = getValue(columnIndex);
        final OpenCypherTypeMapping.Converter<?> converter = getConverter(value);
        return JavaToJdbcTypeConverter.toByteArray(converter.convert(value));
    }

    @Override
    public Date getDate(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a String.", columnIndex);
        final Value value = getValue(columnIndex);
        final OpenCypherTypeMapping.Converter<?> converter = getConverter(value);
        return JavaToJdbcTypeConverter.toDate(converter.convert(value));
    }

    @Override
    public Time getTime(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a String.", columnIndex);
        final Value value = getValue(columnIndex);
        final OpenCypherTypeMapping.Converter<?> converter = getConverter(value);
        return JavaToJdbcTypeConverter.toTime(converter.convert(value));
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a String.", columnIndex);
        final Value value = getValue(columnIndex);
        final OpenCypherTypeMapping.Converter<?> converter = getConverter(value);
        return JavaToJdbcTypeConverter.toTimestamp(converter.convert(value));
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex, final Calendar cal) throws SQLException {
        LOGGER.trace("Getting column {} as a String.", columnIndex);
        final Value value = getValue(columnIndex);
        final OpenCypherTypeMapping.Converter<?> converter = getConverter(value);
        return JavaToJdbcTypeConverter.toTimestamp(converter.convert(value), cal);
    }

    @Override
    public Date getDate(final int columnIndex, final Calendar cal) throws SQLException {
        LOGGER.trace("Getting column {} as a String.", columnIndex);
        final Value value = getValue(columnIndex);
        final OpenCypherTypeMapping.Converter<?> converter = getConverter(value);
        return JavaToJdbcTypeConverter.toDate(converter.convert(value), cal);
    }

    @Override
    public Time getTime(final int columnIndex, final Calendar cal) throws SQLException {
        LOGGER.trace("Getting column {} as a String.", columnIndex);
        final Value value = getValue(columnIndex);
        final OpenCypherTypeMapping.Converter<?> converter = getConverter(value);
        return JavaToJdbcTypeConverter.toTime(converter.convert(value), cal);
    }

    @Override
    public Object getObject(final int columnIndex, final Map<String, Class<?>> map) throws SQLException {
        LOGGER.trace("Getting column {} as an Object using provided Map.", columnIndex);
        final Value value = getValue(columnIndex);
        return getObject(columnIndex, map.get(OpenCypherTypeMapping.BOLT_TO_JDBC_TYPE_MAP.get(value.type()).name()));
    }

    @Override
    public Object getObject(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as an Object.", columnIndex);
        final Value value = getValue(columnIndex);
        final OpenCypherTypeMapping.Converter<?> converter = getConverter(value);
        return converter.convert(value);
    }

    @Override
    public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
        LOGGER.trace("Getting column {} as an Object using provided Type.", columnIndex);
        final Value value = getValue(columnIndex);
        final OpenCypherTypeMapping.Converter<?> converter = getConverter(value);
        return (T) JavaToJdbcTypeConverter.CLASS_CONVERTER_MAP.get(type).function(converter.convert(value));
    }

    protected int getColumnCount() {
        return columns.size();
    }

    private Value getValue(final int columnIndex) throws SQLException {
        verifyOpen();
        validateRowColumn(columnIndex);
        final Value value = rows.get(rowIndex).get(columnIndex);
        wasNull = value.isNull();
        return value;
    }

    private OpenCypherTypeMapping.Converter<?> getConverter(final Value value) {
        return OpenCypherTypeMapping.BOLT_TO_JAVA_TRANSFORM_MAP.get(value.type());
    }
}
