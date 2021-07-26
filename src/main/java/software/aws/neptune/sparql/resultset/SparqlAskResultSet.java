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

package software.aws.neptune.sparql.resultset;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.jdbc.utilities.SqlError;
import software.aws.jdbc.utilities.SqlState;
import software.aws.neptune.common.ResultSetInfoWithoutRows;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class SparqlAskResultSet extends SparqlResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparqlAskResultSet.class);
    private static final String ASK_COLUMN_NAME = "Ask";
    private final Boolean row;
    private final List<String> column;

    /**
     * SparqlResultSet constructor, initializes super class.
     *
     * @param statement     Statement Object.
     * @param resultSetInfo ResultSetInfoWithRows Object.
     */
    public SparqlAskResultSet(final Statement statement, final ResultSetInfoWithRows resultSetInfo) {
        super(statement, resultSetInfo.getColumn(), 1);
        this.row = resultSetInfo.row;
        this.column = resultSetInfo.column;
    }

    /**
     * SparqlResultSet constructor, initializes super class.
     *
     * @param statement                Statement Object.
     * @param resultSetInfoWithoutRows ResultSetInfoWithRows Object.
     */
    public SparqlAskResultSet(final Statement statement, final ResultSetInfoWithoutRows resultSetInfoWithoutRows) {
        super(statement, resultSetInfoWithoutRows.getColumns(), 1);
        this.row = null;
        this.column = resultSetInfoWithoutRows.getColumns();
    }


    @Override
    protected Object getConvertedValue(final int columnIndex) throws SQLException {
        verifyOpen();
        if (columnIndex != 1) {
            throw SqlError
                    .createSQLException(LOGGER, SqlState.DATA_EXCEPTION, SqlError.INVALID_COLUMN_INDEX, columnIndex, 1);
        }
        return row;
    }

    @Override
    protected ResultSetMetaData getResultMetadata() throws SQLException {
        return new SparqlAskResultSetMetadata(column, row.getClass());
    }

    @AllArgsConstructor
    @Getter
    public static class ResultSetInfoWithRows {
        private final Boolean row;
        private final List<String> column = ImmutableList
                .of(ASK_COLUMN_NAME);
        ;
    }
}
