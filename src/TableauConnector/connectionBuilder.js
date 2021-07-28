(function dsbuilder(attr){
    logging.log("entering urlBuilder");

    var urlBuilder = "jdbc:neptune:sqlgremlin://" + attr["server"] + ";port=" + attr["port"];

    var additionalOptions = attr[connectionHelper.attributeVendor1];
    if (additionalOptions) {
        urlBuilder += ";" + additionalOptions;
    }

    logging.log("urlBuilder=" + urlBuilder);
    return [urlBuilder];
})

