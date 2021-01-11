/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 */

package software.amazon.jdbc.utilities;

/**
 * Copy of the java.sql.Types constants but as an enum, for use in lookups.
 */
public enum JdbcType {
    BIT(-7),
    TINYINT(-6),
    SMALLINT(5),
    INTEGER(4),
    BIGINT(-5),
    FLOAT(6),
    REAL(7),
    DOUBLE(8),
    NUMERIC(2),
    DECIMAL(3),
    CHAR(1),
    VARCHAR(12),
    LONGVARCHAR(-1),
    DATE(91),
    TIME(92),
    TIMESTAMP(93),
    BINARY(-2),
    VARBINARY(-3),
    LONGVARBINARY(-4),
    BLOB(2004),
    CLOB(2005),
    BOOLEAN(16),
    ARRAY(2003),
    STRUCT(2002),
    JAVA_OBJECT(2000),
    ROWID(-8),
    NCHAR(-15),
    NVARCHAR(-9),
    LONGNVARCHAR(-16),
    NCLOB(2011),
    SQLXML(2009),
    REF_CURSOR(2012);

    /**
     * The java.sql.Types JDBC type.
     */
    private final int jdbcCode;

    /**
     * JdbcType constructor.
     * @param jdbcCode The java.sql.Types JDBC type associated with this value.
     */
    JdbcType(final int jdbcCode) {
        this.jdbcCode = jdbcCode;
    }
}

