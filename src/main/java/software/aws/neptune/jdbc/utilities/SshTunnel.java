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

package software.aws.neptune.jdbc.utilities;

import com.jcraft.jsch.HostKey;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.regex.Matcher;

public class SshTunnel {
    public static final String SSH_KNOWN_HOSTS_FILE = "~/.ssh/known_hosts";
    public static final String STRICT_HOST_KEY_CHECKING = "StrictHostKeyChecking";
    public static final String HASH_KNOWN_HOSTS = "HashKnownHosts";
    public static final String SERVER_HOST_KEY = "server_host_key";
    public static final String USER_HOME_PROPERTY = "user.home";
    public static final String HOME_PATH_PREFIX_REG_EXPR = "^~[/\\\\].*$";
    public static final String YES = "yes";
    public static final String NO = "no";
    private static final Logger LOGGER = LoggerFactory.getLogger(SshTunnel.class);
    private static final int DEFAULT_PORT = 22;
    private static final String LOCALHOST = "localhost";
    private static final int PORT = 8182;
    private static final int CONNECTION_TIMEOUT_MILLISECONDS = 3000;
    private Integer localPort = null;
    private Session session = null;

    /**
     * Constructor for SshTunnel.
     *
     * @param connectionProperties ConnectionProperties for constructing the ssh tunnel.
     * @throws SQLException If construction fails.
     */
    public SshTunnel(final ConnectionProperties connectionProperties) throws SQLException {
        if (!connectionProperties.enableSshTunnel()) {
            return;
        }

        try {
            // Add private key and optional passphrase.
            final JSch jSch = new JSch();
            jSch.addIdentity(getPath(connectionProperties.getSshPrivateKeyFile()).toString());
            session = jSch.getSession(connectionProperties.getSshUser(), getHostName(connectionProperties),
                    getPort(connectionProperties));
            setHostKeyType(jSch, session, connectionProperties);
            session.connect(CONNECTION_TIMEOUT_MILLISECONDS);

            // Need to force lport because there is port range locks on the Neptune export utility.
            localPort = session.setPortForwardingL(LOCALHOST, 0, connectionProperties.getHostname(),
                    connectionProperties.getPort());
        } catch (final Exception e) {
            localPort = null;
            session = null;
            throw (e instanceof SQLException) ? (SQLException) e : new SQLException(e.getMessage(), e);
        }
    }

    /**
     * Gets an absolute path from the given file path. It performs the substitution for a leading
     * '~' to be replaced by the user's home directory.
     *
     * @param filePath the given file path to process.
     * @return a {@link Path} for the absolution path for the given file path.
     */
    public static Path getPath(final String filePath) {
        if (filePath.matches(HOME_PATH_PREFIX_REG_EXPR)) {
            final String userHomePath = Matcher.quoteReplacement(
                    System.getProperty(USER_HOME_PROPERTY));
            return Paths.get(filePath.replaceFirst("~", userHomePath)).toAbsolutePath();
        }
        return Paths.get(filePath).toAbsolutePath();
    }

    private static int getPort(final ConnectionProperties connectionProperties) {
        final int portSeparatorIndex = connectionProperties.getSshHostname().indexOf(':');
        return (portSeparatorIndex >= 0) ?
                Integer.parseInt(connectionProperties.getSshHostname().substring(portSeparatorIndex + 1)) :
                DEFAULT_PORT;
    }

    private static String getHostName(final ConnectionProperties connectionProperties) {
        final int portSeparatorIndex = connectionProperties.getSshHostname().indexOf(':');
        return (portSeparatorIndex >= 0) ? connectionProperties.getSshHostname().substring(0, portSeparatorIndex)
                : connectionProperties.getSshHostname();
    }

    private static void setHostKeyType(final JSch jSch, final Session session,
                                       final ConnectionProperties connectionProperties)
            throws SQLException {
        // If strict checking is disabled, set it to NO and exit.
        if (!connectionProperties.getSshStrictHostKeyChecking()) {
            session.setConfig(STRICT_HOST_KEY_CHECKING, NO);
            return;
        }

        // Strict checking is enabled, need to get known hosts file.
        final String knowHostsFilename = getPath(StringUtils.isBlank(connectionProperties.getSshKnownHostsFile()) ?
                SSH_KNOWN_HOSTS_FILE : connectionProperties.getSshKnownHostsFile()).toString();
        if (!Files.exists(Paths.get(knowHostsFilename))) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.CONNECTION_EXCEPTION,
                    SqlError.KNOWN_HOSTS_FILE_NOT_FOUND,
                    connectionProperties.getSshKnownHostsFile());
        }

        try {
            jSch.setKnownHosts(knowHostsFilename);
        } catch (final JSchException e) {
            throw new SQLException(e.getMessage(), e);
        }

        final HostKey[] hostKeys = jSch.getHostKeyRepository().getHostKey();
        if (hostKeys.length > 0 && hostKeys[0].getType() != null) {
            session.setConfig(SERVER_HOST_KEY, hostKeys[0].getType());
        }
        session.setConfig(HASH_KNOWN_HOSTS, YES);
    }

    /**
     * Get host for tunnel.
     *
     * @return Host for tunnel.
     */
    public String getTunnelHost() {
        return LOCALHOST;
    }

    /**
     * Get port for tunnel.
     *
     * @return Port for tunnel.
     */
    public int getTunnelPort() {
        return (localPort != null) ? localPort : 0;
    }

    /**
     * Return whether ssh tunnel is valid.
     *
     * @return True if valid, false otherwise.
     */
    public boolean sshTunnelValid() {
        return session != null;
    }

    /**
     * Disconnect SSH tunnel.
     */
    public void disconnect() {
        if (sshTunnelValid()) {
            session.disconnect();
        }
    }
}
