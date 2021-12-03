(function propertiesbuilder(attr) {
    logging.log("entering propertiesBuilder");
    const SSH_USER="sshUser"
    const SSH_HOST="sshHost"
    const SSH_PRIV_KEY_FILE="sshPrivateKeyFile"
    const SSH_PRIVATE_KEY_PASSPHRASE="sshPrivateKeyPassphrase"
    const SSH_STRICT_HOST_KEY_CHECKING="sshStrictHostKeyChecking"
    const SSH_KNOWN_HOSTS_FILE="sshKnownHostsFile"
    const AUTH_SCHEME_KEY = "authScheme";
    const USE_ENCRYPTION_KEY = "enableSsl";
    const SERVICE_REGION_KEY = "serviceRegion";

    var strJSON = JSON.stringify(attr);
    logging.log("connectionProperties attr:" + strJSON);

    // This script is only needed if you are using a JDBC driver.
    var params = {};

    // Set keys for properties needed for connecting using JDBC.

    var authAttrValue = attr[connectionHelper.attributeAuthentication];
    if (authAttrValue == "auth-none")
        params[AUTH_SCHEME_KEY] = "None";
    else if (authAttrValue == "auth-user") {
        params[AUTH_SCHEME_KEY] = "IAMSigV4";
    }

    params[SERVICE_REGION_KEY] = attr["v-service-region"]

    if (attr[connectionHelper.attributeSSLMode] == "require"){
        params[USE_ENCRYPTION_KEY] = "true";
    } else {
        params[USE_ENCRYPTION_KEY] = "false";
    }

    params[SSH_USER] = attr["v-ssh-user"];
    params[SSH_HOST] = attr["v-ssh-host"];
    params[SSH_PRIV_KEY_FILE] = attr["v-ssh-priv-key-filename"];
    params[SSH_PRIVATE_KEY_PASSPHRASE] = attr["v-ssh-priv-key-passphrase"];
    params[SSH_STRICT_HOST_KEY_CHECKING] = attr["v-ssh-strict-host-key-check"];
    params[SSH_KNOWN_HOSTS_FILE] = attr["v-ssh-known-hosts-file"];

    // Format the attributes as 'key=value'. By default some values are escaped or wrapped in curly braces to follow the JDBC standard, but you can also do it here if needed.
    var formattedParams = [];
    for (var key in params) {

        formattedParams.push(connectionHelper.formatKeyValuePair(key, params[key]));
    }

    // Format the additional option 'keys=value' pairs into the connection properties.
    if (attr['v-additional-properties']) {
        var additionalProperties = attr['v-additional-properties'].split(";");
        for (let i = 0; i < additionalProperties.length; i++) {
            const val = additionalProperties[i];
            formattedParams.push(val);
        }
    }

    logging.log("formattedParams: " + formattedParams);
    return formattedParams;
})
