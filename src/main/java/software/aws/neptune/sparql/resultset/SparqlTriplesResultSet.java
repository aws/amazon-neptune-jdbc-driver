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

package software.aws.neptune.sparql.resultset;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.LiteralLabel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.common.ResultSetInfoWithoutRows;
import software.aws.neptune.jdbc.utilities.SqlError;
import software.aws.neptune.jdbc.utilities.SqlState;
import software.aws.neptune.sparql.SparqlTypeMapping;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SparqlTriplesResultSet extends SparqlResultSet {
    public static final String TRIPLES_COLUMN_LABEL_SUBJECT = "Subject";
    public static final String TRIPLES_COLUMN_LABEL_PREDICATE = "Predicate";
    public static final String TRIPLES_COLUMN_LABEL_OBJECT = "Object";
    public static final List<String> TRIPLES_COLUMN_LIST = ImmutableList
            .of(TRIPLES_COLUMN_LABEL_SUBJECT, TRIPLES_COLUMN_LABEL_PREDICATE, TRIPLES_COLUMN_LABEL_OBJECT);
    public static final int TRIPLES_COLUMN_INDEX_SUBJECT = 1;
    public static final int TRIPLES_COLUMN_INDEX_PREDICATE = 2;
    public static final int TRIPLES_COLUMN_INDEX_OBJECT = 3;
    public static final int TRIPLES_COLUMN_COUNT = 3;
    private static final Logger LOGGER = LoggerFactory.getLogger(SparqlTriplesResultSet.class);
    private final List<Triple> rows;
    private final List<String> columns;
    private final List<Object> columnTypes;

    /**
     * SparqlResultSet constructor, initializes super class.
     *
     * @param statement     Statement Object.
     * @param resultSetInfo ResultSetInfoWithRows Object.
     */
    public SparqlTriplesResultSet(final Statement statement, final ResultSetInfoWithRows resultSetInfo) {
        super(statement, resultSetInfo.getColumns(), resultSetInfo.getRows().size());
        this.rows = resultSetInfo.getRows();
        this.columns = resultSetInfo.getColumns();
        this.columnTypes = resultSetInfo.getColumnTypes();
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
        this.columnTypes = columns.stream().map(c -> String.class).collect(Collectors.toList());
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
        if (rows.isEmpty()) {
            // TODO: AN-562 see other ways to address empty result lists
            final List<Object> emptyColumnTypes = new ArrayList<>();
            for (int i = 1; i <= 3; i++) {
                emptyColumnTypes.add(String.class);
            }
            return new SparqlResultSetMetadata(columns, emptyColumnTypes);
        } else {
            return new SparqlResultSetMetadata(columns, this.columnTypes);
        }
    }

    // get the Node of a row of Triple based on given column index
    private Node getNodeFromColumnIndex(final Triple row, final int columnIndex) throws SQLException {
        Node node = null;
        switch (columnIndex) {
            case 1:
                node = row.getSubject();
                break;
            case 2:
                node = row.getPredicate();
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
        private final List<String> columns = TRIPLES_COLUMN_LIST;
        private final List<Object> columnTypes;
    }
}
