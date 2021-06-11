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

package software.amazon.neptune.sparql.resultset;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;
import software.amazon.neptune.sparql.SparqlTypeMapping;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SparqlResultSet extends software.amazon.jdbc.ResultSet implements java.sql.ResultSet {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparqlResultSet.class);
    private final ResultSet result;
    private final List<QuerySolution> rows;
    private final List<String> columns;
    private boolean wasNull = false;

    /**
     * SparqlResultSet constructor, initializes super class.
     *
     * @param statement     Statement Object.
     * @param resultSetInfo ResultSetInfoWithRows Object.
     */
    public SparqlResultSet(final Statement statement, final ResultSetInfoWithRows resultSetInfo) {
        // TODO: fix metadata promotion
        super(statement, resultSetInfo.getColumns(), resultSetInfo.getRows().size());
        this.result = resultSetInfo.getResult();
        this.rows = resultSetInfo.getRows();
        this.columns = resultSetInfo.getColumns();
    }

    @Override
    protected void doClose() throws SQLException {
    }

    @Override
    protected int getDriverFetchSize() throws SQLException {
        // Can't be done based on implementation.
        return 0;
    }

    @Override
    protected void setDriverFetchSize(final int rows) {
        // Can't be done based on implementation.
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Override
    protected ResultSetMetaData getResultMetadata() throws SQLException {
        final List<Class<?>> rowTypes = new ArrayList<>();
        for (final String column : columns) {
            final QuerySolution row = rows.get(0);
            final RDFNode node = row.get(column);
            // TODO: type promotion
            rowTypes.add(getResultClass(node));
        }
        return new SparqlResultSetMetadata(columns, rowTypes);
    }

    @Override
    protected Object getConvertedValue(final int columnIndex) throws SQLException {
        final RDFNode value = getValue(columnIndex);
        if (value == null) {
            return null;
        }
        final Class<?> valueClass = getResultClass(value);
        // Need to check if XSDDateTime types need converting first
        if (SparqlTypeMapping.checkConverter(valueClass)) {
            final SparqlTypeMapping.Converter<?> converter = getConverter(valueClass);
            return converter.convert(value.asLiteral());
        }
        if (SparqlTypeMapping.checkContains(valueClass)) {
            return value.asLiteral().getValue();
        }
        return value.isLiteral() ? value.asLiteral().getLexicalForm() : value.toString();
    }

    // returns the class of an RDF node
    private Class<?> getResultClass(final RDFNode node) {
        Class<?> resultClass;
        if (node == null) {
            return null;
        }
        if (node.isLiteral()) {
            final Literal literal = node.asLiteral();
            resultClass = literal.getDatatype().getJavaClass();
            if (resultClass == null) {
                resultClass = literal.getValue().getClass();
            }
            // We need to then delineate between different XSDDateTime classes
            if (resultClass == org.apache.jena.datatypes.xsd.XSDDateTime.class) {
                resultClass = literal.getDatatype().getClass();
            }
        } else {
            resultClass = node.getClass();
        }
        return resultClass;
    }

    private RDFNode getValue(final int columnIndex) throws SQLException {
        verifyOpen();
        if (rows == null) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.UNSUPPORTED_RESULT_SET_TYPE);
        }
        validateRowColumn(columnIndex);

        final String colName = columns.get(columnIndex - 1);
        final QuerySolution row = rows.get(getRowIndex());
        final RDFNode value = row.get(colName);
        // literal: primitives
        // resource: relationships
        wasNull = (value == null);

        return value;
    }

    private SparqlTypeMapping.Converter<?> getConverter(final Class<?> value) {
        return SparqlTypeMapping.SPARQL_TO_JAVA_TRANSFORM_MAP.get(value);
    }

    @AllArgsConstructor
    @Getter
    public static class ResultSetInfoWithRows {
        private final ResultSet result;
        private final List<QuerySolution> rows;
        private final List<String> columns;
    }
}
