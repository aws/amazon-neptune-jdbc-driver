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

package software.aws.neptune.common;

import com.amazon.neptune.gremlin.driver.sigv4.ChainedSigV4PropertiesProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.neptune.auth.NeptuneNettyHttpSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import org.apache.tinkerpop.gremlin.driver.Cluster;

public class IAMHelper {
    public static void addHandshakeInterceptor(Cluster.Builder builder) {
        builder.handshakeInterceptor( r -> {
            try {
                NeptuneNettyHttpSigV4Signer sigV4Signer =
                        new NeptuneNettyHttpSigV4Signer(
                                new ChainedSigV4PropertiesProvider().getSigV4Properties().getServiceRegion(),
                                new DefaultAWSCredentialsProviderChain());
                sigV4Signer.signRequest(r);
            } catch (NeptuneSigV4SignerException e) {
                throw new RuntimeException("Exception occurred while signing the request", e);
            }
            return r;
        });
    }
}
