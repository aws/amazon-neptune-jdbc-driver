(function propertiesbuilder(attr) {
    logging.log("entering propertiesBuilder");
    var strJSON = JSON.stringify(attr);
    logging.log("connectionProperties attr=" + strJSON);

    // This script is only needed if you are using a JDBC driver.
    var params = {};

    // Set keys for properties needed for connecting using JDBC
    var AUTH_SCHEME_KEY = "authScheme";
    var USE_ENCRYPTION_KEY = "enableSsl";

    var authAttrValue = attr[connectionHelper.attributeAuthentication];
    if (authAttrValue == "auth-none")
        params[AUTH_SCHEME_KEY] = "None";
    else if (authAttrValue == "auth-user") {
        params[AUTH_SCHEME_KEY] = "IAMSigV4";
    }

    if (attr[connectionHelper.attributeSSLMode] == "require"){
        params[USE_ENCRYPTION_KEY] = "true";
    } else {
        params[USE_ENCRYPTION_KEY] = "false";
    }

    // Format the attributes as 'key=value'. By default some values are escaped or wrapped in curly braces to follow the JDBC standard, but you can also do it here if needed.
    var formattedParams = [];
    for (var key in params) {
        formattedParams.push(connectionHelper.formatKeyValuePair(key, params[key]));
    }

    logging.log("formattedParams=" + formattedParams);
    return formattedParams;
})
