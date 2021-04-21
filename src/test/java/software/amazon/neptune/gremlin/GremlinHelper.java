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
 *
 */

package software.amazon.neptune.gremlin;

import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.ConnectionProperties;
import java.util.Properties;
import static software.amazon.neptune.gremlin.GremlinConnectionProperties.CONTACT_POINT_KEY;
import static software.amazon.neptune.gremlin.GremlinConnectionProperties.ENABLE_SSL_KEY;
import static software.amazon.neptune.gremlin.GremlinConnectionProperties.PORT_KEY;
import static software.amazon.neptune.gremlin.GremlinConnectionProperties.SSL_SKIP_VALIDATION_KEY;

public class GremlinHelper {
    /**
     * Function to get properties for Gremlin connection.
     *
     * @param hostname hostname for properties.
     * @param port port number for properties.
     * @return Properties for Gremlin connection.
     */
    public static Properties getProperties(final String hostname, final int port) {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // set default to None
        properties.put(CONTACT_POINT_KEY, hostname);
        properties.put(PORT_KEY, port);
        properties.put(ENABLE_SSL_KEY, false);
        properties.put(SSL_SKIP_VALIDATION_KEY, true);
        return properties;
    }
}
