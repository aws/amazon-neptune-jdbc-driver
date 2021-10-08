/*
 * Copyright <2021> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune.jdbc;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.jdbc.utilities.CastHelper;
import software.aws.neptune.jdbc.utilities.JavaToJdbcTypeConverter;
import software.aws.neptune.jdbc.utilities.SqlError;
import software.aws.neptune.jdbc.utilities.SqlState;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract implementation of ResultSet for JDBC Driver.
 */
public abstract class ResultSet implements java.sql.ResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResultSet.class);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final List<String> columns;
    private final int rowCount;
    private final java.sql.Statement statement;
    @Getter
    private int rowIndex;
    private SQLWarning warnings = null;

    protected ResultSet(final java.sql.Statement statement, final List<String> columns, final int rowCount) {
        this.statement = statement;
        this.columns = columns;
        this.rowCount = rowCount;
        this.rowIndex = -1;
    }

    protected abstract void doClose() throws SQLException;

    protected int getDriverFetchSize() throws SQLException {
        LOGGER.warn("Feature is not supported");
        return 0;
    }

    protected void setDriverFetchSize(final int rows) {
        LOGGER.warn("Feature is not supported");
    }

    protected abstract Object getConvertedValue(int columnIndex) throws SQLException;

    protected abstract ResultSetMetaData getResultMetadata() throws SQLException;

    /**
     * Verify the result set is open.
     *
     * @throws SQLException if the result set is closed.
     */
    protected void verifyOpen() throws SQLException {
        if (isClosed.get()) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.RESULT_SET_CLOSED);
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed.get();
    }

    @Override
    public void close() throws SQLException {
        if (isClosed.getAndSet(true)) {
            return;
        }
        doClose();
    }

    @Override
    public boolean next() throws SQLException {
        LOGGER.trace("Getting next row.");
        // Increment row index, if it exceeds capacity, set it to one after the last element.
        if (++this.rowIndex >= rowCount) {
            this.rowIndex = rowCount;
        }
        return (this.rowIndex < rowCount);
    }

    // Warning implementation.
    @Override
    public SQLWarning getWarnings() throws SQLException {
        verifyOpen();
        return warnings;
    }

    @Override
    public void clearWarnings() {
        warnings = null;
    }

    /**
     * Set a new warning if there were none, or add a new warning to the end of the list.
     *
     * @param warning The {@link SQLWarning} to add.
     */
    protected void addWarning(final SQLWarning warning) {
        LOGGER.warn(warning.getMessage());
        if (this.warnings == null) {
            this.warnings = warning;
            return;
        }

        this.warnings.setNextWarning(warning);
    }

    @Override
    public Statement getStatement() {
        return statement;
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return CastHelper.unwrap(iface, LOGGER, this);
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) {
        return CastHelper.isWrapperFor(iface, this);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        verifyOpen();
        return (getRowIndex() == -1);
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return (getRowIndex() >= rowCount);
    }

    @Override
    public boolean isFirst() throws SQLException {
        return (getRowIndex() == 0);
    }

    @Override
    public int getFetchSize() throws SQLException {
        verifyOpen();
        return getDriverFetchSize();
    }

    @Override
    public void setFetchSize(final int rows) throws SQLException {
        verifyOpen();
        if (rows < 0) {
            throw SqlError.createSQLException(LOGGER, SqlState.DATA_EXCEPTION, SqlError.INVALID_FETCH_SIZE, rows);
        }
        setDriverFetchSize(rows);
    }

    @Override
    public boolean isLast() throws SQLException {
        verifyOpen();
        return (getRowIndex() == (rowCount - 1));
    }

    @Override
    public void beforeFirst() throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void afterLast() throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public boolean first() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public boolean last() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public int getRow() throws SQLException {
        return getRowIndex() + 1;
    }

    @Override
    public boolean absolute(final int row) throws SQLException {
        verifyOpen();
        if (row < 1) {
            throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
        } else if (getRowIndex() > row) {
            throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
        }

        while ((getRowIndex() < row) && next()) {
            continue;
        }
        return !isBeforeFirst() && !isAfterLast();
    }

    @Override
    public int getFetchDirection() {
        return java.sql.ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection(final int direction) throws SQLException {
        if (direction != java.sql.ResultSet.FETCH_FORWARD) {
            throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
        }
    }

    @Override
    public int findColumn(final String columnLabel) throws SQLException {
        final int index = columns.indexOf(columnLabel);
        if (index < 0) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.INVALID_COLUMN_LABEL, columnLabel);
        }
        return columns.indexOf(columnLabel) + 1;
    }

    @Override
    public boolean getBoolean(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a Boolean.", columnIndex);
        return JavaToJdbcTypeConverter.toBoolean(getConvertedValue(columnIndex));
    }

    @Override
    public byte getByte(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a Byte.", columnIndex);
        return JavaToJdbcTypeConverter.toByte(getConvertedValue(columnIndex));
    }

    @Override
    public short getShort(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a Short.", columnIndex);
        return JavaToJdbcTypeConverter.toShort(getConvertedValue(columnIndex));
    }

    @Override
    public int getInt(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a Integer.", columnIndex);
        return JavaToJdbcTypeConverter.toInteger(getConvertedValue(columnIndex));
    }

    @Override
    public long getLong(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a Long.", columnIndex);
        return JavaToJdbcTypeConverter.toLong(getConvertedValue(columnIndex));
    }

    @Override
    public float getFloat(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a Float.", columnIndex);
        return JavaToJdbcTypeConverter.toFloat(getConvertedValue(columnIndex));
    }

    @Override
    public double getDouble(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a Double.", columnIndex);
        return JavaToJdbcTypeConverter.toDouble(getConvertedValue(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a BigDecimal.", columnIndex);
        return JavaToJdbcTypeConverter.toBigDecimal(getConvertedValue(columnIndex));
    }

    @Override
    public String getString(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a String.", columnIndex);
        return JavaToJdbcTypeConverter.toString(getConvertedValue(columnIndex));
    }

    @Override
    public byte[] getBytes(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a byte array.", columnIndex);
        return JavaToJdbcTypeConverter.toByteArray(getConvertedValue(columnIndex));
    }

    @Override
    public Date getDate(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a Date.", columnIndex);
        return JavaToJdbcTypeConverter.toDate(getConvertedValue(columnIndex));
    }

    @Override
    public Time getTime(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a Time.", columnIndex);
        return JavaToJdbcTypeConverter.toTime(getConvertedValue(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as a Timestamp.", columnIndex);
        return JavaToJdbcTypeConverter.toTimestamp(getConvertedValue(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex, final Calendar cal) throws SQLException {
        LOGGER.trace("Getting column {} as a Timestamp.", columnIndex);
        return JavaToJdbcTypeConverter.toTimestamp(getConvertedValue(columnIndex), cal);
    }

    @Override
    public Date getDate(final int columnIndex, final Calendar cal) throws SQLException {
        LOGGER.trace("Getting column {} as a Date with Calendar.", columnIndex);
        return JavaToJdbcTypeConverter.toDate(getConvertedValue(columnIndex), cal);
    }

    @Override
    public Time getTime(final int columnIndex, final Calendar cal) throws SQLException {
        LOGGER.trace("Getting column {} as a Time with Calendar.", columnIndex);
        return JavaToJdbcTypeConverter.toTime(getConvertedValue(columnIndex), cal);
    }

    @Override
    public Object getObject(final int columnIndex) throws SQLException {
        LOGGER.trace("Getting column {} as an Object.", columnIndex);
        return getConvertedValue(columnIndex);
    }

    @Override
    public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
        LOGGER.trace("Getting column {} as an Object using provided Type.", columnIndex);
        if (type == null) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.INVALID_TYPE);
        }
        return (T) JavaToJdbcTypeConverter.CLASS_CONVERTER_MAP.get(type).function(getConvertedValue(columnIndex));
    }

    @Override
    public Object getObject(final int columnIndex, final Map<String, Class<?>> map) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public int getType() throws SQLException {
        return java.sql.ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return java.sql.ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public String getCursorName() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    // Add default not supported for all types.
    @Override
    public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public InputStream getAsciiStream(final int columnIndex) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public InputStream getUnicodeStream(final int columnIndex) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public InputStream getBinaryStream(final int columnIndex) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public Ref getRef(final int columnIndex) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public Blob getBlob(final int columnIndex) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public Clob getClob(final int columnIndex) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public Array getArray(final int columnIndex) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public URL getURL(final int columnIndex) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public RowId getRowId(final int columnIndex) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public NClob getNClob(final int columnIndex) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public SQLXML getSQLXML(final int columnIndex) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public String getNString(final int columnIndex) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public Reader getNCharacterStream(final int columnIndex) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public Reader getCharacterStream(final int columnIndex) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    // Default implementation for all label functions to just use findColumn(label) to find idx and lookup with idx.
    @Override
    public Object getObject(final String columnLabel, final Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel), map);
    }

    @Override
    public Ref getRef(final String columnLabel) throws SQLException {
        return getRef(findColumn(columnLabel));
    }

    @Override
    public Blob getBlob(final String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    @Override
    public Clob getClob(final String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    @Override
    public Array getArray(final String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    @Override
    public String getString(final String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(final String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(final String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(final String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(final String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(final String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(final String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(final String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(final String columnLabel, final int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(final String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(final String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(final String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(final String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(final String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(final String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(final String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public Object getObject(final String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public Reader getCharacterStream(final String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(final String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(final String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
    }

    @Override
    public URL getURL(final String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    @Override
    public RowId getRowId(final String columnLabel) throws SQLException {
        return getRowId(findColumn(columnLabel));
    }

    @Override
    public NClob getNClob(final String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }

    @Override
    public String getNString(final String columnLabel) throws SQLException {
        return getNString(findColumn(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(final String columnLabel) throws SQLException {
        return getNCharacterStream(findColumn(columnLabel));
    }

    @Override
    public Date getDate(final String columnLabel, final Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public Time getTime(final String columnLabel, final Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(final String columnLabel, final Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    @Override
    public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    // All functions below have default implementation which is setup for read only and forward only cursors.
    @Override
    public int getHoldability() throws SQLException {
        return java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean relative(final int rows) throws SQLException {
        verifyOpen();
        if (rows < 0) {
            throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
        }

        int rowCopy = rows;
        while (rowCopy-- > 0) {
            if (!next()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean rowDeleted() {
        return false;
    }

    @Override
    public boolean rowInserted() {
        return false;
    }

    @Override
    public boolean rowUpdated() {
        return false;
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void refreshRow() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public boolean previous() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void insertRow() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void deleteRow() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateArray(final int columnIndex, final Array x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateArray(final String columnLabel, final Array x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x, final int length)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateAsciiStream(final String columnLabel, final InputStream x, final int length)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x, final long length)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateAsciiStream(final String columnLabel, final InputStream x, final long length)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateAsciiStream(final String columnLabel, final InputStream x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateBigDecimal(final int columnIndex, final BigDecimal x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateBigDecimal(final String columnLabel, final BigDecimal x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x, final int i1)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateBinaryStream(final String columnLabel, final InputStream x, final int i)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x, final long length)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateBinaryStream(final String columnLabel, final InputStream x, final long length)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateBinaryStream(final String columnLabel, final InputStream x)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateBlob(final int columnIndex, final Blob x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateBlob(final String columnLabel, final Blob x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateBlob(final int columnIndex, final InputStream x, final long length)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateBlob(final String columnLabel, final InputStream x, final long length)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateBlob(final int columnIndex, final InputStream x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateBlob(final String columnLabel, final InputStream x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateBoolean(final int columnIndex, final boolean x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateBoolean(final String columnLabel, final boolean x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateByte(final int columnIndex, final byte x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateByte(final String columnLabel, final byte x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateBytes(final int columnIndex, final byte[] x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateBytes(final String columnLabel, final byte[] x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x, final int length)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateCharacterStream(final String columnLabel, final Reader x, final int length)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x, final long length)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateCharacterStream(final String columnLabel, final Reader x, final long length)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateCharacterStream(final String columnLabel, final Reader x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateClob(final int columnIndex, final Clob x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateClob(final String columnLabel, final Clob x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateClob(final int columnIndex, final Reader x, final long length)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateClob(final String columnLabel, final Reader x, final long length)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateClob(final int columnIndex, final Reader x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateClob(final String columnLabel, final Reader x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateDate(final int columnIndex, final Date x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateDate(final String columnLabel, final Date x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateDouble(final int columnIndex, final double x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateDouble(final String columnLabel, final double x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateFloat(final int columnIndex, final float x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateFloat(final String columnLabel, final float x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateInt(final int columnIndex, final int x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateInt(final String columnLabel, final int x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateLong(final int columnIndex, final long l) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateLong(final String columnLabel, final long l) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateNCharacterStream(final int columnIndex, final Reader x, final long length)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateNCharacterStream(final String columnLabel, final Reader x, final long length)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateNCharacterStream(final int columnIndex, final Reader x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateNCharacterStream(final String columnLabel, final Reader x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateNClob(final int columnIndex, final NClob nClob) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateNClob(final String columnLabel, final NClob nClob) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateNClob(final int columnIndex, final Reader x, final long length)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateNClob(final String columnLabel, final Reader x, final long length)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateNClob(final int columnIndex, final Reader x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateNClob(final String columnLabel, final Reader x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateNString(final int columnIndex, final String x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateNString(final String columnLabel, final String x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateNull(final int columnIndex) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateNull(final String columnLabel) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateObject(final int columnIndex, final Object x, final int scaleOrLength)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateObject(final int columnIndex, final Object x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateObject(final String columnLabel, final Object x, final int scaleOrLength)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateObject(final String columnLabel, final Object x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateRef(final int columnIndex, final Ref x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateRef(final String columnLabel, final Ref x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateRow() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateRowId(final int columnIndex, final RowId x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateRowId(final String columnLabel, final RowId x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateSQLXML(final int columnIndex, final SQLXML x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateSQLXML(final String columnLabel, final SQLXML x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateShort(final int columnIndex, final short x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateShort(final String columnLabel, final short x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateString(final int columnIndex, final String x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateString(final String columnLabel, final String x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateTime(final int columnIndex, final Time x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateTime(final String columnLabel, final Time x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateTimestamp(final int columnIndex, final Timestamp x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void updateTimestamp(final String columnLabel, final Timestamp x) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    protected void validateRowColumn(final int columnIndex) throws SQLException {
        if ((getRowIndex() < 0) || (getRowIndex() >= rowCount)) {
            throw SqlError
                    .createSQLException(LOGGER, SqlState.DATA_EXCEPTION, SqlError.INVALID_INDEX, getRowIndex() + 1,
                            rowCount);
        }
        if ((columnIndex <= 0) || (columnIndex > columns.size())) {
            throw SqlError
                    .createSQLException(LOGGER, SqlState.DATA_EXCEPTION, SqlError.INVALID_COLUMN_INDEX, columnIndex,
                            columns.size());
        }
    }

    @Override
    public java.sql.ResultSetMetaData getMetaData() throws SQLException {
        return getResultMetadata();
    }
}
