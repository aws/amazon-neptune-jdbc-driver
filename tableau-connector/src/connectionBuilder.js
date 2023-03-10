(function dsbuilder(attr){
    var urlBuilder = "jdbc:neptune:sqlgremlin://" + attr["server"] + ";port=" + attr["port"];

    return [urlBuilder];
})
