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

package software.aws.performance.implementations.executors;

import lombok.SneakyThrows;
import software.aws.performance.PerformanceTestExecutor;

public abstract class JDBCExecutor extends PerformanceTestExecutor {
    abstract java.sql.Statement getNewStatement();

    @Override
    @SneakyThrows
    protected Object execute(final String query) {
        return getNewStatement().executeQuery(query);
    }

    @Override
    @SneakyThrows
    protected int retrieve(final Object retrieveObject) {
        if (!(retrieveObject instanceof java.sql.ResultSet)) {
            throw new Exception("Error: expected a java.sql.ResultSet for data retrieval.");
        }
        final java.sql.ResultSet resultSet = (java.sql.ResultSet) retrieveObject;
        final int columnCount = resultSet.getMetaData().getColumnCount();
        int rowCount = 0;
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                resultSet.getObject(i);
            }
            rowCount++;
        }
        return rowCount;
    }

    @Override
    @SneakyThrows
    protected int retrieveString(final Object retrieveObject) {
        if (!(retrieveObject instanceof java.sql.ResultSet)) {
            throw new Exception("Error: expected a java.sql.ResultSet for data retrieval.");
        }
        final java.sql.ResultSet resultSet = (java.sql.ResultSet) retrieveObject;
        final int columnCount = resultSet.getMetaData().getColumnCount();
        int rowCount = 0;
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                resultSet.getString(i);
            }
            rowCount++;
        }
        return rowCount;
    }

    @Override
    @SneakyThrows
    protected int retrieveInteger(final Object retrieveObject) {
        if (!(retrieveObject instanceof java.sql.ResultSet)) {
            throw new Exception("Error: expected a java.sql.ResultSet for data retrieval.");
        }
        final java.sql.ResultSet resultSet = (java.sql.ResultSet) retrieveObject;
        final int columnCount = resultSet.getMetaData().getColumnCount();
        int rowCount = 0;
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                resultSet.getInt(i);
            }
            rowCount++;
        }
        return rowCount;
    }
}
