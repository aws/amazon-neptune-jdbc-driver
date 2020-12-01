/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 * Copy of the SQLSTATE codes but as an enum, for use in throwing SQLException.
 */
public enum SqlState {
    CONNECTION_EXCEPTION("08000"),
    CONNECTION_FAILURE("08006"),
    DATA_EXCEPTION("22000"),
    DATA_TYPE_TRANSFORM_VIOLATION("0700B"),
    DATA_EXCEPTION_NULL_VALUE("22002"),
    EMPTY_STRING("2200F"),
    FEATURE_NOT_SUPPORTED("0A000"),
    INVALID_AUTHORIZATION_SPECIFICATION("28000"),
    INVALID_QUERY_EXPRESSION("2201S"),
    RESTRICTED_DATA_TYPE_VIOLATION("07006"),
    NUMERIC_VALUE_OUT_OF_RANGE("22003"),
    NO_RESULT_SET_RETURNED("02001"),
    OPERATION_CANCELED("HY008");

    /**
     * The SQLSTATE code.
     */
    private final String sqlState;

    /**
     * SqlState cnstructor.
     * @param sqlState The SQLSTATE code associated with this sql state.
     */
    SqlState(final String sqlState) {
        this.sqlState = sqlState;
    }

    public String getSqlState() {
        return sqlState;
    }
}
