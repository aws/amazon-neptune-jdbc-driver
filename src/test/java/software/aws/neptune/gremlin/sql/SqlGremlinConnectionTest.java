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

package software.aws.neptune.gremlin.sql;

import org.junit.jupiter.api.Test;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SqlGremlinConnectionTest {

    // No compatible mock database at this point so all we can do automated testing on is instantiation.
    @Test
    void testSqlGremlin() throws SQLException {
        final java.sql.Connection connection = DriverManager.getConnection("jdbc:neptune:sqlgremlin://localhost:8181");
        assertTrue(connection instanceof SqlGremlinConnection);
        connection.close();
    }
}
