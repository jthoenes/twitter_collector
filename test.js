var dateFormat = require('dateformat');

function extract_date(date_from_twitter){
    var unixtime = Date.parse(date_from_twitter);
    var date = new Date(unixtime);

    return dateFormat(date, "yyyy-mm-dd'T'HH:MM:sso");
}

var source_date = "Tue Aug 28 19:08:56 +0000 2012";
console.log(extract_date(source_date))