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

package software.aws.neptune.gremlin.adapter;

import org.junit.jupiter.api.Test;
import software.aws.neptune.gremlin.adapter.util.SqlGremlinError;

import java.sql.SQLException;

public class GremlinSqlNotSupportedTest extends GremlinSqlBaseTest {

    GremlinSqlNotSupportedTest() throws SQLException {
    }

    @Override
    protected DataSet getDataSet() {
        return DataSet.SPACE;
    }

    @Test
    public void testOffset() throws SQLException {
        // OFFSET testing - currently not implemented.
        runNotSupportedQueryTestThrows("SELECT name FROM person OFFSET 1", SqlGremlinError.OFFSET_NOT_SUPPORTED);

    }

    @Test
    public void testSubQuery() throws SQLException {
        // Sub Query testing = currently caught by generic catch-all
        runQueryTestThrows("Select name FROM Person WHERE age = (SELECT age FROM person WHERE name = 'Tom')",
                SqlGremlinError.UNKNOWN_OPERATOR, "SCALAR_QUERY");
    }

    @Test
    public void testCast() throws SQLException {
        runNotSupportedQueryTestThrows("SELECT CAST(17 AS varchar)",
                SqlGremlinError.UNSUPPORTED_LITERAL_EXPRESSION);
        runQueryTestThrows("SELECT CAST(person.age as CHAR) FROM person",
                SqlGremlinError.UNKNOWN_OPERATOR, "CAST");
    }
}
