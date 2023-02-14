/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune.jdbc.utilities;

import java.util.ResourceBundle;

/**
 * Enum representing the possible warning messages and lookup facilities for localization.
 */
public enum Warning {
    ERROR_CANCELING_QUERY,
    MAX_VALUE_TRUNCATED,
    VALUE_TRUNCATED,
    NULL_PROPERTY,
    NULL_URL,
    UNSUPPORTED_PROPERTY,
    UNSUPPORTED_URL_PREFIX;

    private static final ResourceBundle RESOURCE = ResourceBundle.getBundle("jdbc");

    /**
     * Looks up the resource bundle string corresponding to the key, and formats it with the provided
     * arguments.
     *
     * @param key        resource key for bundle provided to constructor.
     * @param formatArgs any additional arguments to format the resource string with.
     * @return resource String, formatted with formatArgs.
     */
    public static String lookup(final Warning key, final Object... formatArgs) {
        return String.format(RESOURCE.getString(key.name()), formatArgs);
    }
}
