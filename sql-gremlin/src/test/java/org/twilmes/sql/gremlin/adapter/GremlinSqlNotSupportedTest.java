/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.twilmes.sql.gremlin.adapter;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

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
        runNotSupportedQueryTestThrows("SELECT name FROM person OFFSET 1",
                "Unsupported: OFFSET is not currently supported.");

    }

    @Test
    public void testSubQuery() {
        // Sub Query testing = currently caught by generic catch-all
        runQueryTestThrows("Select name FROM Person WHERE age = (SELECT age FROM person WHERE name = 'Tom')",
                "Error: Unknown operator: %s.");
    }

    // TODO: Handle CAST
    @Disabled
    public void testCast() throws SQLException {
        runNotSupportedQueryTestThrows("SELECT CAST(17 AS varchar)", "");
    }
}
