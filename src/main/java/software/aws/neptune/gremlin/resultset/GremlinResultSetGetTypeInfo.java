/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
 
package software.aws.neptune.gremlin.resultset;

import software.aws.neptune.common.gremlindatamodel.resultset.ResultSetGetTypeInfo;

import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GremlinResultSetGetTypeInfo extends ResultSetGetTypeInfo {
    private static final List<Map<String, Object>> TYPE_INFO = new ArrayList<>();

    static {
        // The order added to TYPE_INFO matters
        putInfo(TYPE_INFO, "Boolean", Types.BIT, false, false);
        putInfo(TYPE_INFO, "Byte", Types.TINYINT, false, true);
        putInfo(TYPE_INFO, "Long", Types.BIGINT, false, true);
        putInfo(TYPE_INFO, "Integer", Types.INTEGER, false, true);
        putInfo(TYPE_INFO, "Short", Types.SMALLINT, false, true);
        putInfo(TYPE_INFO, "Float", Types.REAL, false, true);
        putInfo(TYPE_INFO, "Double", Types.DOUBLE, false, true);
        putInfo(TYPE_INFO, "String", Types.VARCHAR, true, false);
        putInfo(TYPE_INFO, "byte[]", Types.VARCHAR, false, false);
        putInfo(TYPE_INFO, "sql.Date", Types.DATE, false, false);
        putInfo(TYPE_INFO, "util.Date", Types.DATE, false, false);
        putInfo(TYPE_INFO, "Time", Types.TIME, false, false);
        putInfo(TYPE_INFO, "Timestamp", Types.TIMESTAMP, false, false);

        populateConstants(TYPE_INFO);
    }

    public GremlinResultSetGetTypeInfo(final Statement statement) {
        super(statement, new ArrayList<>(TYPE_INFO));
    }
}
