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

package software.aws.neptune.opencypher;

import com.amazonaws.http.HttpMethodName;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.security.InternalAuthToken;

import java.lang.reflect.Type;
import java.net.URI;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.amazonaws.auth.internal.SignerConstants.AUTHORIZATION;
import static com.amazonaws.auth.internal.SignerConstants.HOST;
import static com.amazonaws.auth.internal.SignerConstants.X_AMZ_DATE;
import static com.amazonaws.auth.internal.SignerConstants.X_AMZ_SECURITY_TOKEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.security.InternalAuthToken.CREDENTIALS_KEY;
import static org.neo4j.driver.internal.security.InternalAuthToken.PRINCIPAL_KEY;
import static org.neo4j.driver.internal.security.InternalAuthToken.REALM_KEY;
import static org.neo4j.driver.internal.security.InternalAuthToken.SCHEME_KEY;
import static software.aws.neptune.opencypher.OpenCypherIAMRequestGenerator.DUMMY_USERNAME;
import static software.aws.neptune.opencypher.OpenCypherIAMRequestGenerator.HTTP_METHOD_HDR;
import static software.aws.neptune.opencypher.OpenCypherIAMRequestGenerator.SERVICE_NAME;

class OpenCypherIAMRequestGeneratorTest {
    private static final String DUMMY_ACCESS_KEY_ID = "xxx";
    private static final String DUMMY_SESSION_TOKEN = "zzz";

    @BeforeAll
    public static void beforeAll() {
        System.setProperty("aws.accessKeyId", DUMMY_ACCESS_KEY_ID);
        System.setProperty("aws.secretKey", "yyy");
    }

    @AfterAll
    public static void afterAll() {
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretKey");
    }

    @AfterEach
    public void afterEach() {
        System.clearProperty("aws.sessionToken");
    }

    @Test
    public void testCreateAuthToken() {
        verifyAuthToken(false);
    }

    @Test
    public void testCreateAuthTokenWithTempCreds() {
        System.setProperty("aws.sessionToken", DUMMY_SESSION_TOKEN);
        verifyAuthToken(true);
    }

    private void verifyAuthToken(final boolean useTempCreds) {
        final String url = "bolt://somehost.com:58763";
        final String region = "us-west-2";

        final AuthToken authToken = OpenCypherIAMRequestGenerator.createAuthToken(url, region);
        assertTrue(authToken instanceof InternalAuthToken);
        final Map<String, Value> internalAuthToken = ((InternalAuthToken) authToken).toMap();

        assertEquals(value("basic"), internalAuthToken.get(SCHEME_KEY));
        assertEquals(value(DUMMY_USERNAME), internalAuthToken.get(PRINCIPAL_KEY));

        assertFalse(internalAuthToken.containsKey(REALM_KEY));

        assertTrue(internalAuthToken.containsKey(CREDENTIALS_KEY));
        final Value credentialsValue = internalAuthToken.get(CREDENTIALS_KEY);
        final Type type = new TypeToken<Map<String, String>>() {
        }.getType();
        final Map<String, String> credentials = new Gson().fromJson(credentialsValue.asString(), type);

        assertNotNull(credentials.get(X_AMZ_DATE));
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssX");
        final ZonedDateTime xAmzDate = ZonedDateTime.parse(credentials.get(X_AMZ_DATE), formatter);
        final ZonedDateTime refDateTime = ZonedDateTime.now(ZoneOffset.UTC);
        final long dateTimeDiff = Duration.between(xAmzDate, refDateTime).getSeconds();
        assertTrue(dateTimeDiff >= 0 && dateTimeDiff < 60);

        final URI uri = URI.create(url);
        assertEquals(String.format("%s:%d", uri.getHost(), uri.getPort()), credentials.get(HOST));

        assertEquals(HttpMethodName.GET.name(), credentials.get(HTTP_METHOD_HDR));

        assertEquals(useTempCreds ? DUMMY_SESSION_TOKEN : null, credentials.get(X_AMZ_SECURITY_TOKEN));

        final String auth = credentials.get(AUTHORIZATION);
        assertNotNull(auth);
        assertTrue(auth.startsWith("AWS4-HMAC-SHA256"));
        final Pattern pattern = Pattern.compile("([^,\\s]+)=([^,\\s]+)");
        final Matcher matcher = pattern.matcher(auth);
        final Map<String, String> keyValues = new HashMap<>();
        while (matcher.find()) {
            assertEquals(2, matcher.groupCount());
            final String key = matcher.group(1);
            final String value = matcher.group(2);
            keyValues.put(key, value);
        }
        assertNotNull(keyValues.get("Signature"));

        assertEquals(
                String.format(
                        "%s/%s/%s/%s/aws4_request",
                        DUMMY_ACCESS_KEY_ID,
                        DateTimeFormatter.ofPattern("yyyyMMdd").format(refDateTime),
                        region,
                        SERVICE_NAME
                ),
                keyValues.get("Credential")
        );

        final String signedHeaders = keyValues.get("SignedHeaders");
        assertNotNull(signedHeaders);
        final List<String> headers = Arrays.asList(signedHeaders.split(";"));
        assertTrue(headers.contains(HOST.toLowerCase()));
        assertTrue(headers.contains(X_AMZ_DATE.toLowerCase()));
        assertEquals(useTempCreds, headers.contains(X_AMZ_SECURITY_TOKEN.toLowerCase()));
    }
}
