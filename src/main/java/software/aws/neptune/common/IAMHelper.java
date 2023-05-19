package software.aws.neptune.common;

import com.amazon.neptune.gremlin.driver.sigv4.ChainedSigV4PropertiesProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.neptune.auth.NeptuneNettyHttpSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import org.apache.tinkerpop.gremlin.driver.Cluster;

public class IAMHelper {
    public static void addHandshakeInterceptor(Cluster.Builder builder) {
        builder.handshakeInterceptor( r ->
                {
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
                }
        );
    }
}
