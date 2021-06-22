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

package software.amazon.neptune.sparql;

public class SparqlStatementTestBase {
    protected static final String QUICK_QUERY;
    protected static final String LONG_QUERY;

    static {
        QUICK_QUERY = "SELECT * { ?s ?p ?o } LIMIT 100";
        // TODO put in long query for cancellation testing after implementing ResultSet
        LONG_QUERY = "TODO";
    }
}
