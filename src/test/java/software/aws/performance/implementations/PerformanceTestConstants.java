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

package software.aws.performance.implementations;

import software.aws.neptune.jdbc.utilities.AuthScheme;

public class PerformanceTestConstants {
    public static final String ENDPOINT = "no-auth-oc-enabled.cluster-cdubgfjknn5r.us-east-1.neptune.amazonaws.com";
    public static final String REGION = "us-east-1";
    public static final AuthScheme AUTH_SCHEME = AuthScheme.None;
    public static final int PORT = 8182;
    public static final int LIMIT_COUNT = 1000;
}
