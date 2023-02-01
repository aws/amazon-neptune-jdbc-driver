/*
 * Copyright <2023> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import software.aws.neptune.common.gremlindatamodel.resultset.ResultSetGetCatalogs;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SparqlResultSetGetCatelogs extends ResultSetGetCatalogs {
    /**
     * ResultSetGetCatalogs constructor, initializes super class.
     *
     * @param statement Statement Object.
     */
    public SparqlResultSetGetCatelogs(final Statement statement) {
        super(statement);
    }

    @Override
    protected ResultSetMetaData getResultMetadata() throws SQLException {
        final List<Object> rowTypes = new ArrayList<>();
        for (int i = 0; i < getColumns().size(); i++) {
            rowTypes.add(String.class);
        }
        return new SparqlResultSetMetadata(getColumns(), rowTypes);
    }
}
