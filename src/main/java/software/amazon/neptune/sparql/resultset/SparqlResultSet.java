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

package software.amazon.neptune.sparql.resultset;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;
import software.amazon.neptune.common.ResultSetInfoWithoutRows;
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

    /**
     * SparqlResultSet constructor, initializes super class.
     *
     * @param statement     Statement Object.
     * @param resultSetInfoWithoutRows ResultSetInfoWithRows Object.
     */
    public SparqlResultSet(final Statement statement, final ResultSetInfoWithoutRows resultSetInfoWithoutRows) {
        super(statement, resultSetInfoWithoutRows.getColumns(), resultSetInfoWithoutRows.getRowCount());
        this.result = null;
        this.rows = null;
        this.columns = resultSetInfoWithoutRows.getColumns();
    }

    @Override
    protected void doClose() throws SQLException {
    }

    @Override
    protected int getDriverFetchSize() throws SQLException {
        // TODO: AN-562 possibly raise error instead of leaving as a comment
        // Can't be done based on implementation.
        return 0;
    }

    @Override
    protected void setDriverFetchSize(final int rows) {
        // TODO: AN-562 possibly raise error instead of leaving as a comment
        // Can't be done based on implementation.
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Override
    protected ResultSetMetaData getResultMetadata() throws SQLException {
        final List<Object> rowTypes = new ArrayList<>();
        for (final String column : columns) {
            final QuerySolution row = rows.get(0);
            final RDFNode node = row.get(column);
            // TODO: AN-562 find efficient type promotion with row looping
            if (node == null) {
                rowTypes.add(null);
            } else {
                rowTypes.add(node.isLiteral() ? node.asLiteral().getDatatype() : node.getClass());
            }
        }
        return new SparqlResultSetMetadata(columns, rowTypes);
    }

    @Override
    protected Object getConvertedValue(final int columnIndex) throws SQLException {
        final RDFNode node = getValue(columnIndex);
        if (node == null) {
            return null;
        }
        if (node.isLiteral()) {
            final Literal literal = node.asLiteral();
            final RDFDatatype resultDatatype = literal.getDatatype();
            try {
                // check for types that needs conversion first
                if (SparqlTypeMapping.checkConverter((XSDDatatype) resultDatatype)) {
                    final SparqlTypeMapping.Converter<?> converter = getConverter((XSDDatatype) resultDatatype);
                    return converter.convert(literal);
                }
                if (SparqlTypeMapping.checkContains((XSDDatatype) resultDatatype)) {
                    return literal.getValue();
                }
            } catch (final ClassCastException e) {
                LOGGER.warn("Value is not of typed literal XSDDatatype, returning as String");
                return literal.getLexicalForm();
            }

            return literal.getLexicalForm();
        }
        return node.toString();
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

    private SparqlTypeMapping.Converter<?> getConverter(final XSDDatatype datatype) {
        return SparqlTypeMapping.SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.get(datatype);
    }

    @AllArgsConstructor
    @Getter
    public static class ResultSetInfoWithRows {
        private final ResultSet result;
        private final List<QuerySolution> rows;
        private final List<String> columns;
    }
}
