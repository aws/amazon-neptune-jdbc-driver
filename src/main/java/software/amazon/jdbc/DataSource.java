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
 *
 */

package software.amazon.jdbc;

import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.CastHelper;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * Abstract implementation of DataSource for JDBC Driver.
 */
public abstract class DataSource implements javax.sql.DataSource, javax.sql.ConnectionPoolDataSource {
    private static final java.util.logging.Logger LOGGER = Logger.getLogger(DataSource.class.toString());

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return CastHelper.unwrap(iface, LoggerFactory.getLogger(DataSource.class), this);
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) {
        return CastHelper.isWrapperFor(iface, this);
    }

    @Override
    public PrintWriter getLogWriter() {
        return null;
    }

    @Override
    public void setLogWriter(final PrintWriter out) {
        // NOOP
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return LOGGER;
    }

    // TODO: Get and set of different properties. Either done generically through this class or custom in OpenCypher implementation.
}
