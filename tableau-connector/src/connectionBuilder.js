(function dsbuilder(attr){
    var urlBuilder = "jdbc:neptune:sqlgremlin://" + attr["server"] + ";port=" + attr["port"];

    var additionalOptions = attr['v-additional-properties'];
    if (additionalOptions) {
        urlBuilder += ";" + additionalOptions;
    }

    return [urlBuilder];
})
