var short_url = "http://www.northampton.gov.uk/downloads/file/5060/northampton_hod_2012_pdf";
var long_url = short_url;

var redis = require("redis");
var $log = require('nlogger').logger(module);

var reddis_client = undefined;

var reddis_connect = function () {
    reddis_client = redis.createClient();

    reddis_client.on("error", function (err) {
        $log.error("Error in connecting to Reddis: " + err);
    });
}

reddis_connect();

reddis_client.hset("urls", short_url, long_url);
reddis_client.hget("urls", short_url, function(err, result){
   $log.warn(result);
});