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

package software.aws.neptune.opencypher.utilities;

import java.util.ArrayList;
import java.util.List;

public class OpenCypherGetColumnUtilities {
    public static final List<String> COLUMN_NAMES = new ArrayList<>();

    static {
        COLUMN_NAMES.add("TABLE_CAT");
        COLUMN_NAMES.add("TABLE_SCHEM");
        COLUMN_NAMES.add("TABLE_NAME");
        COLUMN_NAMES.add("COLUMN_NAME");
        COLUMN_NAMES.add("DATA_TYPE");
        COLUMN_NAMES.add("TYPE_NAME");
        COLUMN_NAMES.add("COLUMN_SIZE");
        COLUMN_NAMES.add("BUFFER_LENGTH");
        COLUMN_NAMES.add("DECIMAL_DIGITS");
        COLUMN_NAMES.add("NUM_PREC_RADIX");
        COLUMN_NAMES.add("NULLABLE");
        COLUMN_NAMES.add("REMARKS");
        COLUMN_NAMES.add("COLUMN_DEF");
        COLUMN_NAMES.add("SQL_DATA_TYPE");
        COLUMN_NAMES.add("SQL_DATETIME_SUB");
        COLUMN_NAMES.add("CHAR_OCTET_LENGTH");
        COLUMN_NAMES.add("ORDINAL_POSITION");
        COLUMN_NAMES.add("IS_NULLABLE");
        COLUMN_NAMES.add("SCOPE_CATALOG");
        COLUMN_NAMES.add("SCOPE_SCHEMA");
        COLUMN_NAMES.add("SCOPE_TABLE");
        COLUMN_NAMES.add("SOURCE_DATA_TYPE");
        COLUMN_NAMES.add("IS_AUTOINCREMENT");
        COLUMN_NAMES.add("IS_GENERATEDCOLUMN");
    }
}
