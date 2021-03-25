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
 */

package sample.applications;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This class provides some examples on how to send queries to Amazon Neptune with the JDBC driver.
 */
public class QueryExecutionExamples {
    private final Connection connection;

    /**
     * Constructor initializes the {@link java.sql.Connection}.
     *
     * @throws SQLException If creating the {@link Connection} fails.
     */
    QueryExecutionExamples() throws SQLException {
        connection = AuthenticationExamples.createIAMAuthConnection();
    }

    /**
     * Helper function to execute queries.
     *
     * @param query Query to execute.
     * @return {@link java.sql.ResultSet} after executing query.
     * @throws SQLException If {@link java.sql.Statement} creation or query execution fails.
     */
    ResultSet executeQuery(final String query) throws SQLException {
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(query);
        statement.close();
        return resultSet;
    }

    /**
     * This function sends a query with a Boolean literal and returns the value as a Boolean.
     *
     * @throws SQLException If {@link java.sql.Statement} creation or query execution fails.
     */
    void queryLiteralBooleanExample() throws SQLException {
        final String query = "RETURN true AS n";
        final ResultSet resultSet = executeQuery(query);

        // Value will be true.
        final Boolean value = resultSet.getBoolean(0);
        resultSet.close();
    }

    /**
     * This function sends a query with a Integer literal and returns the value as a Integer.
     *
     * @throws SQLException If {@link java.sql.Statement} creation or query execution fails.
     */
    void queryLiteralIntegerExample() throws SQLException {
        final String query = "RETURN 1 AS n";
        final ResultSet resultSet = executeQuery(query);

        // Value will be 1.
        final Integer value = resultSet.getInt(0);
        resultSet.close();
    }

    /**
     * This function sends a query with a Float literal and returns the value as a Float.
     *
     * @throws SQLException If {@link java.sql.Statement} creation or query execution fails.
     */
    void queryLiteralFloatExample() throws SQLException {
        final String query = "RETURN 1.0 AS n";
        final ResultSet resultSet = executeQuery(query);

        // Value will be 1.0.
        final Float value = resultSet.getFloat(0);
        resultSet.close();
    }

    /**
     * This function sends a query with a String literal and returns the value as a String.
     *
     * @throws SQLException If {@link java.sql.Statement} creation or query execution fails.
     */
    void queryLiteralStringExample() throws SQLException {
        final String query = "RETURN 'hello' AS n";
        final ResultSet resultSet = executeQuery(query);

        // Value will be "hello".
        final String value = resultSet.getString(0);
        resultSet.close();
    }

    /**
     * This function sends a query with a Date literal and returns the value as a Date.
     *
     * @throws SQLException If {@link java.sql.Statement} creation or query execution fails.
     */
    void queryLiteralDateExample() throws SQLException {
        final String query = "RETURN date(\"1993-03-30\") AS n";
        final ResultSet resultSet = executeQuery(query);

        // Value will be the date 1993-03-03 (yyyy-mm-dd).
        final java.sql.Date value = resultSet.getDate(0);
        resultSet.close();
    }

    /**
     * This function sends a query with a node and returns the value as a String.
     *
     * @throws SQLException If {@link java.sql.Statement} creation or query execution fails.
     */
    void queryNodeExample() throws SQLException {
        final String query = "CREATE (node:Foo {hello:'world'}) RETURN node";
        final ResultSet resultSet = executeQuery(query);

        // Value will be the String "([Foo] : {hello=world})".
        final String value = resultSet.getString(0);
        resultSet.close();
    }

    /**
     * This function sends a query with a relationship and returns the value as a String.
     *
     * @throws SQLException If {@link java.sql.Statement} creation or query execution fails.
     */
    void queryRelationshipExample() throws SQLException {
        final String query = "CREATE (node1:Foo)-[rel:Rel {hello:'world'}]->(node2:Bar) RETURN rel";
        final ResultSet resultSet = executeQuery(query);
        // Value will be the String "[Rel : {hello=world}]".
        final String value = resultSet.getString(0);
        resultSet.close();
    }

    /**
     * This function sends a query with a path and returns the value as a String.
     *
     * @throws SQLException If {@link java.sql.Statement} creation or query execution fails.
     */
    void queryPathExample() throws SQLException {
        final String query = "CREATE p=(l:Person { name:'Foo'})-[:WORKS {position:'developer'}]->(bqt:Company " +
                "{product:'software'})<-[:WORKS {position:'developer'}]-(v:Person { name:'Bar'}) RETURN p";
        final ResultSet resultSet = executeQuery(query);
        // Value will be the String
        // "([Person] : {name=Foo})-[WORKS : {position=developer0}]->([Company] : {product=software})
        //  <-[WORKS : {position=developer}]-([Person] : {name=Bar})".
        final String value = resultSet.getString(0);
        resultSet.close();
    }
}
