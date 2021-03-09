(function propertiesbuilder(attr) {
    var strJSON = JSON.stringify(attr);
    logging.log("connectionProperties attr=" + strJSON);

    // This script is only needed if you are using a JDBC driver.
    var params = {};

    //set keys for properties needed for connecting using JDBC
    var AUTH_SCHEME_KEY = "AuthScheme";
    var USE_ENCRYPTION_KEY = "UseEncryption";
    var LOG_LEVEL_KEY = "LogLevel";
    var CONNECTION_TIMEOUT_MILLIS_KEY = "ConnectionTimeout";
    var CONNECTION_RETRY_COUNT_KEY = "ConnectionRetryCount";
    var CONNECTION_POOL_SIZE_KEY = "ConnectionPoolSize";

    var authAttrValue = attr[connectionHelper.attributeAuthentication];
    if (authAttrValue == "NONE")
        params[AUTH_SCHEME_KEY] = "None";
    else if (authAttrValue == "AWS_SIGV4") {
        params[AUTH_SCHEME_KEY] = "IAMSigV4";
    }

    if (attr[connectionHelper.attributeSSLMode] == "require"){
        params[USE_ENCRYPTION_KEY] = "true";
    } else {
        params[USE_ENCRYPTION_KEY] = "false";
    }

    params[LOG_LEVEL_KEY] = attr[connectionHelper.attributeVendor1];

    //Set hardcoded default values.
    params[CONNECTION_TIMEOUT_MILLIS_KEY] = "5000";
    params[CONNECTION_RETRY_COUNT_KEY] = "1";
    params[CONNECTION_POOL_SIZE_KEY] = "1000";

    // Format the attributes as 'key=value'. By default some values are escaped or wrapped in curly braces to follow the JDBC standard, but you can also do it here if needed.
    var formattedParams = [];
    for (var key in params) {
        formattedParams.push(connectionHelper.formatKeyValuePair(key, params[key]));
    }

    logging.log("formattedParams=" + formattedParams);
    return formattedParams;
})
