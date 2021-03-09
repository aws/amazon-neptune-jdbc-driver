(function dsbuilder(attr){
    var urlBuilder = "jdbc:neptune:opencypher://bolt://" + attr["server"] + ":" + attr["port"];
    logging.log("urlBuilder=" + urlBuilder);
    return [urlBuilder];
})

