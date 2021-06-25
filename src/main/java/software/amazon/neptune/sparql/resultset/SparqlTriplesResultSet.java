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

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.LiteralLabel;
import org.neo4j.driver.internal.types.InternalTypeSystem;
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

public class SparqlTriplesResultSet extends SparqlResultSet {
    public static final String TRIPLES_COLUMN_NAME_SUBJECT = "Subject";
    public static final String TRIPLES_COLUMN_NAME_PREDICATE = "Predicate";
    public static final String TRIPLES_COLUMN_NAME_OBJECT = "Object";
    public static final int TRIPLES_COLUMN_INDEX_SUBJECT = 1;
    public static final int TRIPLES_COLUMN_INDEX_PREDICATE = 2;
    public static final int TRIPLES_COLUMN_INDEX_OBJECT = 3;
    public static final int TRIPLES_COLUMN_COUNT = 3;
    private static final Logger LOGGER = LoggerFactory.getLogger(SparqlTriplesResultSet.class);
    private final List<Triple> rows;
    private final List<String> columns;

    /**
     * SparqlResultSet constructor, initializes super class.
     *
     * @param statement     Statement Object.
     * @param resultSetInfo ResultSetInfoWithRows Object.
     */
    public SparqlTriplesResultSet(final Statement statement, final ResultSetInfoWithRows resultSetInfo) {
        // TODO: fix metadata promotion
        super(statement, resultSetInfo.getColumns(), resultSetInfo.getRows().size());
        this.rows = resultSetInfo.getRows();
        this.columns = resultSetInfo.getColumns();
    }

    /**
     * SparqlResultSet constructor, initializes super class.
     *
     * @param statement                Statement Object.
     * @param resultSetInfoWithoutRows ResultSetInfoWithRows Object.
     */
    public SparqlTriplesResultSet(final Statement statement, final ResultSetInfoWithoutRows resultSetInfoWithoutRows) {
        super(statement, resultSetInfoWithoutRows.getColumns(), resultSetInfoWithoutRows.getRowCount());
        this.rows = null;
        this.columns = resultSetInfoWithoutRows.getColumns();
    }

    @Override
    protected Object getConvertedValue(final int columnIndex) throws SQLException {
        final Node node = getValue(columnIndex);
        if (node == null) {
            return null;
        }
        if (node.isLiteral()) {
            final LiteralLabel literal = node.getLiteral();
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

    private Node getValue(final int columnIndex) throws SQLException {
        verifyOpen();
        if (rows.isEmpty()) {
            // TODO: AN-562 is this the best error to throw to address empty result lists?
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.UNSUPPORTED_RESULT_SET_TYPE);
        }
        validateRowColumn(columnIndex);

        final Triple row = rows.get(getRowIndex());
        final Node value = getNodeFromColumnIndex(row, columnIndex);
        setWasNull(value == null);

        return value;
    }

    private SparqlTypeMapping.Converter<?> getConverter(final XSDDatatype datatype) {
        return SparqlTypeMapping.SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.get(datatype);
    }

    @Override
    protected ResultSetMetaData getResultMetadata() throws SQLException {
        final List<Object> rowTypes = new ArrayList<>();
        if (rows.isEmpty()) {
            // TODO: AN-562 see other ways to address empty result lists
            for (int i = 1; i <= 3; i++) {
                rowTypes.add(InternalTypeSystem.TYPE_SYSTEM.STRING());
            }
        } else {
            for (int i = 1; i <= 3; i++) {
                // TODO: AN-562 find efficient type promotion with row looping
                final Triple row = rows.get(0);
                final Node node = getNodeFromColumnIndex(row, i);
                if (node == null) {
                    rowTypes.add(null);
                } else {
                    rowTypes.add(node.isLiteral() ? node.getLiteral().getDatatype() : node.getClass());
                }
            }
        }
        return new SparqlResultSetMetadata(columns, rowTypes);
    }

    // get the Node of a row of Triple based on given column index
    private Node getNodeFromColumnIndex(final Triple row, final int columnIndex) throws SQLException {
        Node node = null;
        switch (columnIndex) {
            case 1:
                node = row.getPredicate();
                break;
            case 2:
                node = row.getSubject();
                break;
            case 3:
                node = row.getObject();
                break;
            default:
                throw SqlError
                        .createSQLException(LOGGER, SqlState.DATA_EXCEPTION, SqlError.INVALID_COLUMN_INDEX,
                                columnIndex);
        }

        return node;
    }

    @AllArgsConstructor
    @Getter
    public static class ResultSetInfoWithRows {
        private final List<Triple> rows;
        private final List<String> columns = ImmutableList
                .of(TRIPLES_COLUMN_NAME_SUBJECT, TRIPLES_COLUMN_NAME_PREDICATE, TRIPLES_COLUMN_NAME_OBJECT);
    }
}
