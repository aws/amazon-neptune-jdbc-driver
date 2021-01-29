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

package software.amazon.neptune.opencypher.resultset;

import org.neo4j.driver.Record;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.Type;
import java.util.List;

public class OpenCypherResultSetMetadataStringTypes extends OpenCypherResultSetMetadata {

    /**
     * OpenCypherResultSetMetadataStringTypes constructor, initializes super class.
     *
     * @param columns List of ordered columns.
     * @param rows    List of rows.
     */
    public OpenCypherResultSetMetadataStringTypes(final List<String> columns,
                                                  final List<Record> rows) {
        super(columns, rows);
    }

    /**
     * Get Bolt type of a given column.
     *
     * @param column the 1-based column index.
     * @return Bolt Type Object for column.
     */
    @Override
    protected Type getColumnBoltType(final int column) {
        return InternalTypeSystem.TYPE_SYSTEM.STRING();
    }
}
