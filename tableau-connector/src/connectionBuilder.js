(function dsbuilder(attr){
    logging.log("entering urlBuilder");

    var urlBuilder = "jdbc:neptune:sqlgremlin://" + attr["server"] + ";port=" + attr["port"];

    logging.log("urlBuilder=" + urlBuilder);
    return [urlBuilder];
})
