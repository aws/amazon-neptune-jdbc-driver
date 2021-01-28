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

import com.google.common.collect.ImmutableList;
import org.neo4j.driver.Record;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OpenCypherResultSetGetTableTypes extends OpenCypherResultSetGetString {
    /**
     * TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     */
    private static final List<String> COLUMNS = ImmutableList.of("TABLE_TYPE");
    private static final List<Record> ROWS = new ArrayList<>();
    private static final Map<String, String> CONVERSION_MAP = new HashMap<>();

    static {
        // Add null to ROWS so size is 1 for internal usage purposes.
        ROWS.add(null);
        CONVERSION_MAP.put("TABLE_TYPE", "TABLE");
    }

    /**
     * OpenCypherResultSet constructor, initializes super class.
     *
     * @param statement Statement Object.
     */
    public OpenCypherResultSetGetTableTypes(final Statement statement) {
        super(statement, ROWS, COLUMNS, CONVERSION_MAP);
    }
}
