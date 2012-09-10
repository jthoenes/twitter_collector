var ntwitter = require('ntwitter');
var request = require("request");
var Q = require("q");
var _ = require('underscore');
var amqp = require('amqp');
var dateFormat = require('dateformat');
var $log = require('nlogger').logger(module);
var redis = require("redis");
var url = require('url');

var GOVUK = GOVUK || {};
GOVUK.Insights = GOVUK.Insights || {};
GOVUK.Insights.TwitterCollector = function () {

    const GOVUK_HOSTS = [
        // -- GOV.UK
        "gov.uk",
        "www.gov.uk",
        // -- Direct GOV
        "direct.gov.uk",
        "www.direct.gov.uk",
        "jobseekers.direct.gov.uk",
        "taxdisc.direct.gov.uk",
        "epetitions.direct.gov.uk",
        "studentfinance.direct.gov.uk",
        "local.direct.gov.uk",
        "dvlaregistrations.direct.gov.uk",
        "motinfo.direct.gov.uk",
        "dwp-services.direct.gov.uk",
        "monitoring.direct.gov.uk",
        // -- Business Link
        'businesslink.gov.uk',
        'www.businesslink.gov.uk',
        'online.businesslink.gov.uk',
        'contractfinder.businesslink.gov.uk',
        'improve.businesslink.gov.uk',
        'edon.businesslink.gov.uk',
        'tariff.businesslink.gov.uk',
        'events.businesslink.gov.uk',
        'elearning.businesslink.gov.uk',
        'ukwelcome.businesslink.gov.uk'
    ];
    const MAX_PAGE = 20;
    const SEARCH_TERM = 'gov.uk';
    const STREAM_TRACK = 'govuk';
    const STREAM_FOLLOW = '460116600,268349503,771955915';
    const SEARCH_TIMEOUT = 2 * 60 * 1000;
    const URL_HASHSET = "short_urls";
    const KEY_SINCE_ID = "govuk_twitter_since_id";

    var twitter = undefined;
    var amqp_exchange = undefined;
    var amqp_connection = undefined;
    var redis_client = undefined;

    var create_twitter = function () {
        $log.debug("Connecting to Twitter ...");
        var deferred = Q.defer();
        var _twitter = new ntwitter({
            consumer_key:process.env.TWITTER_CONSUMER_KEY,
            consumer_secret:process.env.TWITTER_CONSUMER_SECRET,
            access_token_key:process.env.TWITTER_ACCESS_TOKEN_KEY,
            access_token_secret:process.env.TWITTER_ACCESS_TOKEN_SECRET
        })
        _twitter.verifyCredentials(function () {
            twitter = _twitter;
            deferred.resolve(true);
            $log.debug("Connected to Twitter");
        });
        return deferred.promise;
    }

    var create_amqp = function () {
        $log.debug("Connecting to AMQP ...");
        var deferred = Q.defer();
        var _amqp_connection = amqp.createConnection({host:"localhost"});
        _amqp_connection.on('ready', function () {
            _amqp_connection.exchange('datainsight', {type:'topic'}, function (_amqp_exchange) {
                amqp_connection = _amqp_connection;
                amqp_exchange = _amqp_exchange;
                deferred.resolve(true);
                $log.debug("Connected to AMQP");
            });

        });
        return deferred.promise;
    };

    var create_redis = function () {
        $log.debug("Connecting to Redis ...");
        var _redis_client = redis.createClient();

        var deferred = Q.defer();
        _redis_client.on("ready", function () {
            redis_client = _redis_client;
            deferred.resolve(true);
            $log.debug("Connected to Redis");
        });
        _redis_client.on("error", function (err) {
            deferred.reject(err);
            $log.error("Error in connecting to Reddis: " + err);
        });
        return deferred.promise;
    };

    var publish = function (amqp_topic, message) {
        $log.debug("Publish tweet {} to {}", message.payload.tweet_id, amqp_topic);
        amqp_exchange.publish(amqp_topic, message);
    };

    var process_stream_tweet = function (tweet) {
        extract_payload('twitter_stream', tweet, function (payload) {
            $log.debug("Handling stream tweet {}", payload.tweet_id);
            publish("twitter.stream_results", create_message(payload));
        });
    };

    var create_message = function(payload) {
        return {envelope:{
            collected_at:format_date(new Date()),
            collector:"Twitter (NodeJS)"
        },
            payload:payload
        };
    }

    var process_search_tweet = function (tweet) {
        extract_payload('twitter_search', tweet, function (payload) {
            $log.debug("Handling search tweet {}", payload.tweet_id);
            $log.trace(payload);
            // searching based on path, if GOV.UK is not mentioned, don't send the message
            if (!_.isEmpty(payload.paths)) {
                publish("twitter.search_results", create_message(payload));
            } else {
                $log.debug("Reject search tweet {} for urls {}", payload.tweet_id, payload.urls);
            }
        });
    };


    var extract_payload = function (type, tweet, callback) {
        // We have to use string as id, because javascript is not able to handle numbers so big :-(
        var payload = {
            tweet_id:tweet.id_str,
            type:type,
            user_id:tweet.from_user_id_str || tweet.user.id_str,
            username:tweet.from_user || tweet.user.screen_name,
            text:tweet.text,
            geo:tweet.geo,
            coordinates:tweet.coordinates,
            time:extract_date(tweet.created_at),
            mentions:tweet.entities.user_mentions.map(function (user_mention) {
                return user_mention.screen_name;
            }),
            hashtags:tweet.entities.hashtags.map(function (hashtag) {
                return hashtag.text;
            })
        }

        payload.link = "https://twitter.com/" + payload.username + "/status/" + payload.tweet_id

        extract_urls_and_paths(tweet.entities, function (urls, paths) {
            payload.urls = urls;
            payload.paths = paths;
            callback(payload);
        })
    };

    var format_date = function (date) {
        return dateFormat(date, "yyyy-mm-dd'T'HH:MM:sso");
    }

    var extract_date = function (date_from_twitter) {
        var unixtime = Date.parse(date_from_twitter);
        var date = new Date(unixtime);

        return format_date(date);
    }

    var extract_urls_and_paths = function (entities, callback) {
        var response_promises = entities.urls.map(function (url) {
            return url.expanded_url
        }).map(resolve_url);
        Q.all(response_promises).then(function (full_urls) {
            resolved_urls = full_urls.map(function (full_url) {
                return full_url;
            });
            paths = extract_govuk_paths(full_urls);
            callback(resolved_urls, paths);
        });
    };

    var extract_govuk_paths = function (urls) {
        return _.reject(urls.map(function (url_str) {
            var uri = url.parse(url_str);
            if (_.include(GOVUK_HOSTS, uri.hostname)) {
                return uri.pathname;
            }
        }), function (path) {
            return path == undefined
        });
    }

    var resolve_url = function (source_url) {
        var deferred = Q.defer();
        if (source_url != null) {
            redis_client.hget(URL_HASHSET, source_url, function (err, full_url) {
                if (full_url) {
                    $log.debug("Using cache for {} -> {}.", source_url, full_url);
                    deferred.resolve(full_url);
                } else {
                    $log.debug("Loading {} from server.", source_url);
                    request({ method:"HEAD", url:source_url, followAllRedirects:true },
                        function (error, response) {
                            if (error) {
                                deferred.reject(undefined);
                            } else {
                                var full_url = response.request.href;
                                redis_client.hset(URL_HASHSET, source_url, full_url);
                                deferred.resolve(full_url);
                            }
                        });
                }
            });
        } else {
            deferred.reject(undefined);
        }
        return deferred.promise;
    }


    var execute_search = function (params, callback) {
        params = _.extend({result_type:'recent', rpp:100, include_entities:true}, params)
        twitter.search(SEARCH_TERM, params, function (err, data) {
            if (err) {
                $log.error("An error occured searching for tweets: " + err);
                callback({});
            } else {
                $log.info("Executed search: {}, results: {}", params, data.results.length);
                callback(data);
            }
        })
    }

    var handle_first_search_result = function (data, since_id) {
        if (data && data.results) {

            data.results.forEach(process_search_tweet);

            if (data.results.length >= (data.results_per_page - 10)) {
                execute_search({page:2, max_id:since_id}, handle_next_search_result)
            }
        } else {
            $log.warn("Unprocessable search result: {}", data);
        }
    }

    var handle_next_search_result = function (data) {
        if (data && data.results) {
            data.results.forEach(process_search_tweet);

            if (data.results.length != 0) {
                var page = data.page + 1;
                if (page <= MAX_PAGE) {
                    execute_search({page:page, max_id:data.max_id}, handle_next_search_result)
                }
            }
        } else {
            $log.warn("Unprocessable search result: {}", data);
        }
    }

    var start_search = function (callback) {
        $log.info("Starting search ...");
        redis_client.get(KEY_SINCE_ID, function (err, since_id) {
            since_id = since_id || 0;
            execute_search({since_id:since_id}, function (data) {

                handle_first_search_result(data, since_id);

                since_id = data.max_id_str;
                redis_client.set(KEY_SINCE_ID, since_id);

                callback();
            });
        });
    }

    var init_search = function () {
        start_search(function () {
            setTimeout(init_search, SEARCH_TIMEOUT);
        });
    }

    var init_stream = function () {
        $log.info("Starting twitter stream ...");
        twitter.stream('statuses/filter', {'track':STREAM_TRACK, 'follow':STREAM_FOLLOW}, function (stream) {
            stream.on('data', function (tweet) {
                process_stream_tweet(tweet)
            })
            $log.info("Twitter stream started!");
        })
    };

    this.start = function () {
        Q.all([create_twitter(), create_amqp(), create_redis()]).then(function (args) {
            $log.info("Started Twitter & AMQP connection.");
            init_stream();
            init_search();
        })
    };

    this.stop = function () {
        if (amqp_connection) {
            amqp_connection.end();
        }
        if (redis_client) {
            redis_client.quit();
        }
    };

};

var collector = new GOVUK.Insights.TwitterCollector();
collector.start();

process.on('SIGINT', function () {
    $log.info('Closing Twitter Collector');
    collector.stop();
    process.exit(0)
});
