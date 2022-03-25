/*
 * Copyright <2022> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package sample.applications;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This class provides some examples on how to get metadata on the database. Because Amazon Neptune is a Graph database
 * a mapping to a relational database is done. The distinct label sets of the nodes are provided as tables, and the
 * properties of the nodes are provided as columns in the tables. Clashing types on properties are resolved by promoting
 * the column to a String type. The getSchemas and getCatalogs functions return Empty {@java.sql.ResultSet}'s because
 * there is no mapping from a graph to either of these relational concepts.
 */
public class MetadataExamples {
    private final DatabaseMetaData databaseMetaData;

    /**
     * Constructor initializes connection and gets the {@link DatabaseMetaData}.
     *
     * @throws SQLException If creating the {@link Connection} or getting the {@link DatabaseMetaData} fails.
     */
    MetadataExamples() throws SQLException {
        final Connection connection = AuthenticationExamples.createIAMAuthConnection();
        databaseMetaData = connection.getMetaData();
    }

    /**
     * This function retrieves and iterates over the {@link ResultSet} returned from
     * {@link DatabaseMetaData#getColumns(String, String, String, String)}
     *
     * @throws SQLException If getting the {@link ResultSet} from the {@link DatabaseMetaData} fails.
     */
    public void getColumnsMetadataExample() throws SQLException {
        final ResultSet getColumnsResultSet = databaseMetaData.getColumns(null, null, null, null);
        if (!getColumnsResultSet.next()) {
            throw new SQLException(
                    "This graph contains no columns (properties on any nodes with distinct label sets).");
        }

        do {
            // Grab the table name, column name, and data type of column. Look here for more information:
            // https://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html#getColumns(java.lang.String,%20java.lang.String,%20java.lang.String,%20java.lang.String)
            final String tableName = getColumnsResultSet.getString("TABLE_NAME");
            final String columnName = getColumnsResultSet.getString("COLUMN_NAME");
            final String dataType = getColumnsResultSet.getString("DATA_TYPE");
        } while (getColumnsResultSet.next());
    }

    /**
     * This function retrieves and iterates over the {@link ResultSet} returned from
     * {@link DatabaseMetaData#getTables(String, String, String, String[])}
     *
     * @throws SQLException If getting the {@link ResultSet} from the {@link DatabaseMetaData} fails.
     */
    public void getTablesMetadataExample() throws SQLException {
        final ResultSet getTablesResultSet = databaseMetaData.getTables(null, null, null, null);
        if (!getTablesResultSet.next()) {
            throw new SQLException("This graph contains no tables (nodes with distinct label sets).");
        }

        do {
            // Grab the table name. Look here for more information:
            // https://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html#getTables(java.lang.String,%20java.lang.String,%20java.lang.String,%20java.lang.String[])
            final String tableName = getTablesResultSet.getString("TABLE_NAME");
        } while (getTablesResultSet.next());
    }
}
