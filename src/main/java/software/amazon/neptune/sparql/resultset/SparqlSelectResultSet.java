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
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.neptune.common.ResultSetInfoWithoutRows;
import software.amazon.neptune.sparql.SparqlTypeMapping;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SparqlSelectResultSet extends SparqlResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparqlSelectResultSet.class);
    private final List<QuerySolution> rows;
    private final List<String> columns;
    private final List<Object> columnTypes;

    /**
     * SparqlResultSet constructor, initializes super class.
     *
     * @param statement     Statement Object.
     * @param resultSetInfo ResultSetInfoWithRows Object.
     */
    public SparqlSelectResultSet(final Statement statement, final ResultSetInfoWithRows resultSetInfo) {
        super(statement, resultSetInfo.getColumns(), resultSetInfo.getRows().size());
        this.rows = resultSetInfo.getRows();
        this.columns = resultSetInfo.getColumns();
        this.columnTypes = resultSetInfo.getColumnTypes();
    }

    /**
     * SparqlResultSet constructor, initializes super class.
     *
     * @param statement                Statement Object.
     * @param resultSetInfoWithoutRows ResultSetInfoWithoutRows Object.
     */
    public SparqlSelectResultSet(final Statement statement, final ResultSetInfoWithoutRows resultSetInfoWithoutRows) {
        super(statement, resultSetInfoWithoutRows.getColumns(), resultSetInfoWithoutRows.getRowCount());
        this.rows = null;
        this.columns = resultSetInfoWithoutRows.getColumns();
        this.columnTypes = null;
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
        validateRowColumn(columnIndex);

        final String colName = columns.get(columnIndex - 1);
        final QuerySolution row = rows.get(getRowIndex());
        final RDFNode value = row.get(colName);
        // literal: primitives
        // resource: relationships
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
            for (final String column : columns) {
                emptyColumnTypes.add(InternalTypeSystem.TYPE_SYSTEM.STRING());
            }
            return new SparqlResultSetMetadata(columns, emptyColumnTypes);
        } else {
            return new SparqlResultSetMetadata(columns, this.columnTypes);
        }
    }

    @AllArgsConstructor
    @Getter
    public static class ResultSetInfoWithRows {
        private final List<QuerySolution> rows;
        private final List<String> columns;
        private final List<Object> columnTypes;
    }
}
