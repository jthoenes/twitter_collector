var ntwitter = require('ntwitter');
var request = require("request");
var Q = require("q");
var fs = require("fs");
var _ = require('underscore');

var GOVUK = GOVUK || {};
GOVUK.Insights = GOVUK.Insights || {};
GOVUK.Insights.TwitterCollector = function(){
	
	const GOVUK_HOSTS = ["www.gov.uk", "gov.uk", "direct.gov.uk", "epetitions.direct.gov.uk", "www.direct.gov.uk"];
	const SINCE_ID_FILE = "/tmp/_govuk_twitter_collector_search_since_id"
	const MAX_PAGE = 5
	
	var self = this;
	var twitter = new ntwitter({
	  consumer_key: process.env.TWITTER_CONSUMER_KEY,
	  consumer_secret: process.env.TWITTER_CONSUMER_SECRET,
	  access_token_key: process.env.TWITTER_ACCESS_TOKEN_KEY,
	  access_token_secret: process.env.TWITTER_ACCESS_TOKEN_SECRET
	});

	var process_search_tweet = function(tweet) {
		extract_payload('twitter_search', tweet, function(payload){
			// searching based on path, if GOV.UK is not mentioned, don't send the message
			if(!_.isEmpty(payload.paths)){
				console.log(payload);
			} else {
				console.log("Skipped: " + payload.urls.join());
			}
		});
	};
	
	
	
	var extract_payload = function(type, tweet, callback){
		var payload = {
			tweet_id: tweet.id,
			type: type,
			user_id: tweet.from_user_id,
			username: tweet.from_user, 
			text: tweet.text,
			time: Date.parse(tweet.created_at),
			hashtags: tweet.entities.hashtags.map(function(hashtag){ return hashtag.text; })
		}
		
		payload.uri = "https://twitter.com/"+payload.username+"/status/"+payload.tweet_id
		
		extract_urls_and_paths(tweet.entities, function(urls, paths){
			payload.urls = urls;
			payload.paths = paths;
			callback(payload);
		})
	};
		
	var extract_urls_and_paths = function(entities, callback){
		var response_promises = entities.urls.map(function(url){ return url.expanded_url}).map(resolve_url);
		Q.all(response_promises).then(function(responses){
			resolved_urls = responses.map(function(r){return r.request.href});
			paths = extract_govuk_paths(responses);
			callback(resolved_urls, paths)
		});
	};
	
	var extract_govuk_paths = function(responses){
		return _.reject(responses.map(function(response){
			var uri = response.request.uri;
			if(_.include(GOVUK_HOSTS, uri.host)){
				return uri.path;
			}	
		}), function(path){return path == undefined});
	}

	var resolve_url = function(url){
		var deferred = Q.defer();
		request( { method: "HEAD", url: url, followAllRedirects: true },
		function (error, response) {
			if (error) {
				deferred.reject(undefined);
			} else {
				deferred.resolve(response);
			}
		});
		return deferred.promise;
	}
	
	var verify_with_twitter = function(){
		twitter.verifyCredentials(function(err, data){});
	}
	
	var execute_search = function(params, callback){
		params = _.extend({result_type: 'recent', rpp: 100, include_entities: true}, params)
		twitter.search('gov.uk', params, function(err, data){
			console.log("Read tweets: " + data.results.length)
			if(err){
				console.log("An error occured searching for tweets: " + err)
			} else {
				callback(data);
			}
		})
	}
	
	var handle_first_search_result = function(data, since_id){
		data.results.forEach(process_search_tweet);
		
		if(data.results.length == data.results_per_page){
			execute_search({page: 2, max_id: since_id}, handle_next_search_result)
		}
	}
	
	var handle_next_search_result = function(data){
		data.results.forEach(process_search_tweet);
		
		if(data.results.length == data.results_per_page){
			var page = data.page+1
			if(page <= MAX_PAGE) {
				execute_search({page: page, max_id: data.max_id}, handle_next_search_result)
			}
		}
	}
	
	this.search = function(){
		since_id = parseInt(fs.readFileSync(SINCE_ID_FILE)) || 0;
		execute_search({since_id: since_id}, function(data){
			
			handle_first_search_result(data, since_id);
			
			since_id = data.max_id
			fs.writeFileSync(SINCE_ID_FILE, since_id);
		});
	}

};

var collector = new GOVUK.Insights.TwitterCollector()
collector.search();
/*.stream('statuses/filter', {'track':'govuk,mytw'}, function(stream) {
  console.log("waiting");
  stream.on('data', function (data) {
    console.log("By @" + data.user.screen_name + " at " + data.created_at);
    console.log("Text:" + data.text);
    console.log("Urls:" + data.entities.urls.map(function(url){return url.expanded_url}).join());
    console.log("Hashtags:" +  data.entities.hashtags.map(function(hashtag){return hashtag.text}).join());
	console.log("----- ----- ----- ----- ----- ----- ----- -----");
  });
});*/
