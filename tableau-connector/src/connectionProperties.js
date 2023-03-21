(function propertiesbuilder(attr) {

    const AUTH_SCHEME_KEY = "authScheme";
    const USE_ENCRYPTION_KEY = "enableSsl";
    const SERVICE_REGION_KEY = "serviceRegion";
    const LOG_LEVEL="logLevel";
    const SCAN_TYPE="scanType";
    const CONN_TIMEOUT="connectionTimeout";
    const CONN_RETRY="connectionRetryCount";

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

    params[LOG_LEVEL] = attr["v-log-level"];
    params[SCAN_TYPE] = attr["v-scan-type"];
    params[CONN_TIMEOUT] = attr["v-connection-timeout"];
    params[CONN_RETRY] = attr["v-connection-retry"];

    return params;
})
