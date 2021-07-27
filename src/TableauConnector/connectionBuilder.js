(function dsbuilder(attr){
    logging.log("~~~~~~~~~~~~~~~~");
    var urlBuilder = "jdbc:neptune:sqlgremlin://" + attr["server"] + ";port=" + attr["port"];
    logging.log("~~~~~~~~~~~~~~~~=" + urlBuilder);
    return [urlBuilder];
})

