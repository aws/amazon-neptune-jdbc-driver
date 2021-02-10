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
 * Auth Scheme enum.
 */
public enum AuthScheme {
    IAMSigV4("IAMSigV4"),
    IAMRoles("IAMRole"),
    None("None");

    private final String stringValue;

    AuthScheme(final String stringValue) {
        this.stringValue = stringValue;
    }

    /**
     * Converts case-insensitive string to enum value.
     * @param in The case-insensitive string to be converted to enum.
     * @return The enum value if string is recognized as a valid value, otherwise null.
     */
    public static AuthScheme fromString(final String in) {
        for (AuthScheme scheme : AuthScheme.values()) {
            if (scheme.stringValue.equalsIgnoreCase(in)) {
                return scheme;
            }
        }
        return null;
    }

    @Override
    public java.lang.String toString() {
        return this.stringValue;
    }
}
