/*
 * Copyright <2022> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune.opencypher;

import com.amazonaws.DefaultRequest;
import com.amazonaws.Request;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.HttpMethodName;
import com.google.gson.Gson;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.auth.internal.SignerConstants.AUTHORIZATION;
import static com.amazonaws.auth.internal.SignerConstants.HOST;
import static com.amazonaws.auth.internal.SignerConstants.X_AMZ_DATE;
import static com.amazonaws.auth.internal.SignerConstants.X_AMZ_SECURITY_TOKEN;

/**
 * Class to help with IAM authentication.
 */
public class OpenCypherIAMRequestGenerator {
    private static final AWSCredentialsProvider AWS_CREDENTIALS_PROVIDER = new DefaultAWSCredentialsProviderChain();
    static final String SERVICE_NAME = "neptune-db";
    static final String HTTP_METHOD_HDR = "HttpMethod";
    static final String DUMMY_USERNAME = "username";
    private static final Gson GSON = new Gson();

    /**
     * Function to generate AuthToken using IAM authentication.
     *
     * @param url    URL to point at.
     * @param region Region to use.
     * @return AuthToken for IAM authentication.
     */
    public static AuthToken createAuthToken(final String url, final String region) {
        final Request<Void> request = new DefaultRequest<>(SERVICE_NAME);
        request.setHttpMethod(HttpMethodName.GET);
        request.setEndpoint(URI.create(url));
        // Comment out the following line if you're using an engine version older than 1.2.0.0
        // request.setResourcePath("/opencypher");

        final AWS4Signer signer = new AWS4Signer();
        signer.setRegionName(region);
        signer.setServiceName(request.getServiceName());
        signer.sign(request, AWS_CREDENTIALS_PROVIDER.getCredentials());

        return AuthTokens.basic(DUMMY_USERNAME, getAuthInfoJson(request));
    }

    private static String getAuthInfoJson(final Request<Void> request) {
        final Map<String, Object> obj = new HashMap<>();
        obj.put(AUTHORIZATION, request.getHeaders().get(AUTHORIZATION));
        obj.put(HTTP_METHOD_HDR, request.getHttpMethod());
        obj.put(X_AMZ_DATE, request.getHeaders().get(X_AMZ_DATE));
        obj.put(HOST, request.getHeaders().get(HOST));
        obj.put(X_AMZ_SECURITY_TOKEN, request.getHeaders().get(X_AMZ_SECURITY_TOKEN));

        return GSON.toJson(obj);
    }
}
