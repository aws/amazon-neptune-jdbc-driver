(function dsbuilder(attr){
    var urlBuilder = "jdbc:neptune:opencypher://bolt://" + attr["server"] + ":" + attr["port"];

    var additionalOptions = attr[connectionHelper.attributeVendor1];
    if (additionalOptions) {
        urlBuilder += ";" + additionalOptions;
    }

    logging.log("urlBuilder=" + urlBuilder);
    return [urlBuilder];
})

