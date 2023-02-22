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

package software.aws.neptune.gremlin.adapter.util;

import java.sql.SQLException;

/**
 * SQLException for failures specifically relating to not supported scenarios.
 */
public class SQLNotSupportedException extends SQLException {

    /**
     * SQLNotSupportedException constructor.
     *
     * @param message   Message of the exception.
     * @param cause     Underlying cause for the exception.
     */
    public SQLNotSupportedException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * SQLNotSupportedException constructor.
     *
     * @param message   Message of the exception.
     */
    public SQLNotSupportedException(final String message) {
        super(message);
    }
}
