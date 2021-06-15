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

package software.amazon.neptune.common;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * Class to provide an empty {@link java.sql.ResultSet}.
 */
public class EmptyResultSet extends software.amazon.jdbc.ResultSet implements java.sql.ResultSet {

    /**
     * Initialize the EmptyResultSet. {@link java.sql.Statement} is required as an input.
     *
     * @param statement {@link java.sql.Statement} to initialize with.
     */
    public EmptyResultSet(final Statement statement) {
        super(statement, null, 0);
    }

    @Override
    protected void doClose() throws SQLException {
    }

    @Override
    protected int getDriverFetchSize() throws SQLException {
        return 0;
    }

    @Override
    protected void setDriverFetchSize(final int rows) {
    }

    @Override
    protected Object getConvertedValue(final int columnIndex) throws SQLException {
        return null;
    }

    @Override
    protected ResultSetMetaData getResultMetadata() throws SQLException {
        return new EmptyResultSetMetadata(new ArrayList<>(), new ArrayList<>());
    }

    @Override
    public boolean wasNull() throws SQLException {
        return false;
    }
}
