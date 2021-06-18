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

package software.amazon.neptune.gremlin.resultset;

import software.amazon.neptune.common.gremlindatamodel.resultset.ResultSetGetSchemas;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Gremlin ResultSet class for getSchemas.
 */
public class GremlinResultSetGetSchemas extends ResultSetGetSchemas {
    /**
     * Constructor for GremlinResultSetGetSchemas.
     *
     * @param statement Statement Object.
     */
    public GremlinResultSetGetSchemas(final Statement statement) {
        super(statement);
    }

    @Override
    protected ResultSetMetaData getResultMetadata() {
        final List<Class<?>> rowTypes = new ArrayList<>();
        for (int i = 0; i < getColumns().size(); i++) {
            rowTypes.add(String.class);
        }
        return new GremlinResultSetMetadata(getColumns(), rowTypes);
    }
}
