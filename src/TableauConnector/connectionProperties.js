(function propertiesbuilder(attr) {
    // This script is only needed if you are using a JDBC driver.
    var params = {};

    // Set keys for properties needed for connecting using JDBC.
    params["SERVER"] = attr[connectionHelper.attributeServer];

    // Format the attributes as 'key=value'. By default some values are escaped or wrapped in curly braces to follow the JDBC standard, but you can also do it here if needed.
    var formattedParams = [];
    for (var key in params) {
        formattedParams.push(connectionHelper.formatKeyValuePair(key, params[key]));
    }

    // The result will look like (search for jdbc-connection-properties in tabprotosrv.log):
    //"jdbc-connection-properties","v":{"password":"********","s3_staging_dir":"s3://aws-athena-s3/","user":"admin"}
    return formattedParams;
})
