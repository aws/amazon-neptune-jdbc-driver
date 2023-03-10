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

    // Format the attributes as 'key=value'. By default some values are escaped or wrapped in curly braces to follow the JDBC standard, but you can also do it here if needed.
    // var formattedParams = [];
    // for (var key in params) {
    //     formattedParams.push(connectionHelper.formatKeyValuePair(key, params[key]));
    // }

    // Format the additional option 'keys=value' pairs into the connection properties.
    // if (attr['v-additional-properties']) {
    //     var additionalProperties = attr['v-additional-properties'].split(";");
    //     for (var i = 0; i < additionalProperties.length; i++) {
    //         const val = additionalProperties[i];
    //         formattedParams.push(val);
    //     }
    // }

    // if (attr['v-additional-properties']) {
    //     var additionalProperties = attr['v-additional-properties'].split(";");
    //     for (var i = 0; i < additionalProperties.length; i++) {
    //         var property = additionalProperties[i].split("=");
    //         params[property[0]] = property[1];
    //     }
    // }

    return params;
})
