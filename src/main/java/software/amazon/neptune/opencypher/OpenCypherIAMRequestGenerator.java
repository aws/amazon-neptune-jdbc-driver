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

package software.amazon.neptune.opencypher;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.neptune.auth.NeptuneNettyHttpSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import com.google.gson.Gson;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to help with IAM authentication.
 */
public class OpenCypherIAMRequestGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCypherIAMRequestGenerator.class);
    private static final AWSCredentialsProvider AWS_CREDENTIALS_PROVIDER = new DefaultAWSCredentialsProviderChain();
    private static final Gson GSON = new Gson();

    /**
     * Function to generate AuthToken using IAM authentication.
     *
     * @param url    URL to point at.
     * @param region Region to use.
     * @return AuthToken for IAM authentication.
     * @throws SQLException If request cannot be generated.
     */
    public static AuthToken getSignedHeader(final String url, final String region) throws SQLException {
        final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                io.netty.handler.codec.http.HttpMethod.GET, url);
        try {
            new NeptuneNettyHttpSigV4Signer(region, AWS_CREDENTIALS_PROVIDER).signRequest(request);
        } catch (final NeptuneSigV4SignerException e) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.CONNECTION_EXCEPTION,
                    SqlError.FAILED_TO_OBTAIN_AUTH_TOKEN, e);
        }

        final Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("Authorization", request.headers().get("Authorization"));
        requestMap.put("HttpVersion", HttpVersion.HTTP_1_1.text());
        requestMap.put("HttpMethod", io.netty.handler.codec.http.HttpMethod.GET.name());
        requestMap.put("X-Amz-Date", request.headers().get("X-Amz-Date"));
        requestMap.put("Host", url);

        // Convert Map to HTTP form and send as AuthToken password.
        return AuthTokens.basic("", GSON.toJson(requestMap));
    }
}
