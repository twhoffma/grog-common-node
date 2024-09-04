require('http').globalAgent.maxSockets = 5;
require('https').globalAgent.maxSockets = 5;

request = require('request');
q = require('q');
logger = require('./logger.js');

fs = require('fs');
c = JSON.parse(fs.readFileSync('localconfig.json', 'utf8'));

var maxRequests = 1;
var maxRequestsPerSec = 1;
var maxNumBackoffs = 5;
var numRequests = 0;
var reqQueue = [];

function qrequest(method, url, data, headers, use_fallback, fallback_iter, useQueue, isQueued, lastResult){
	var p;
			
	use_fallback = use_fallback || false;
	fallback_iter = fallback_iter || 0;
	method = method.toUpperCase();
	isQueued = isQueued || false;
	useQueue = useQueue || false;
	lastResult = lastResult || 0;
	
	if(method === "GET"){
		if(numRequests < maxRequests || !useQueue){
			if(fallback_iter === 0 && useQueue){
				numRequests++;
			}
			
			p = runRequest(reqGet, 0, url);
		}else{
			var d = q.defer();
			
			reqQueue.push({"url": url, "deferred": d});
			logger.debug("++ " + reqQueue.length + " left in queue. "); // [" + url + "]");	
			logger.debug("Queued " + url);	
			
			p = d.promise;
		}
	}else if(method === "POST"){
		p = reqPost(url, data, headers)
	}else if(method === "PUT"){
		p = reqPut(url, data)
	}else if(method.toUpperCase() === "DELETE"){
		p = reqDelete(url, data)
	}else{
		p.reject("Unknown http verb");
	}
	
	if(useQueue){	
		//TODO: make num requests domain dependent.
		return p.finally(function(){
			runNextRequest();
		}).catch(function(e){
			
			throw e
		});
	}else{
		return p
	}
}

function runNextRequest(){
	var qr;
	
	numRequests--;
	
	if(reqQueue.length > 0){
		qr = reqQueue.shift();
		logger.debug("-- " + reqQueue.length + " left in queue.");	
		
		//return runRequest(reqGet, 0, qr.url).then(
		runRequest(reqGet, 0, qr.url).then(
			function(v){
				qr.deferred.resolve(v);	
			},
			function(e){
				qr.deferred.reject(e);
			}
		).finally(function(){
			runNextRequest();
		});
	}
}

function queueRequest(fn, url){
	return new q.Promise(function(resolve, reject){
		reqQueue.push({"url": url, "deferred": d});
		logger.debug(reqQueue.length + " left in queue. "); 
	})
}

function runRequest(fn, tries, url, data, header){
	return new q.Promise(function(resolve, reject){
		var delay = 0;
		if(tries > maxNumBackoffs){
			reject("Max number of backoffs reached!");
		}else{
			if(tries > 0){
				delay = (1 + Math.random())*Math.exp(tries+1);
				logger.debug("Delay added: " + delay);
			}
			
			q.delay(1000 * delay).then(function(){
				return fn(url, data, header)
			}).then(
				function(v){
					resolve(v);
				},
				function(e){
					if(e == 202 || e == 503){
						return runRequest(fn, tries+1, url, data, header)
					}else{
						reject(e);
					}		
				}
			).then(
				function(v){
					resolve(v);
				},
				function(e){
					logger.error("runRequest failed");
					reject(e);
				}
			);
		}
	})
}

function reqGet(url){
	return new q.Promise(function(resolve, reject){
		request({
					method: "GET",
					uri: url
					//,timeout: 120000
				}, 
				function(error, response, body){
			if(!error && response.statusCode == 200){
				resolve(response.body);
			}else if(!error && response.statusCode == 202){
				//The request was accepted. This implies server rendering. Try back-off.
				reject(202);
			}else{
				if((error && error.code == 'ECONNRESET') || (response && response.statusCode && response.statusCode == 503)){
					reject(503);
				}else if(error && error.code == 'ETIMEDOUT'){
					logger.error("Connection time out? " + (err.connect === true));
					reject(504);
				}else{
					logger.error("reqGet failed: " + url);
					logger.error(error||response.statusCode)
					reject(error||response.statusCode);
				}
			}
		});
	})
}

function reqDelete(url, data){
	return new q.Promise(function(resolve, reject){
			request(
				{
					method: "DELETE",
					uri: url,
					body: data
				},
				function(error, response, body) {
					if(response.statusCode == 200){
						resolve("OK");
					}else{
						logger.error("DELETE error: " + response.statusCode);
						logger.error("DELETE error: " + body);
						logger.error(method.toUpperCase() + " url: " + url);
							
						reject("ERROR");
					}
				}
			);
		}
	)
}

function reqPut(url, data, headers){
	var r = {
			method: 'PUT',
			uri: url
	};
	
	if(data !== undefined && data !== null){
			r['body'] = data;
	}
		
	if(headers !== undefined & headers !== null){
		r['headers'] = headers;
	}
	
	return new q.Promise(function(resolve, reject){
			request(
				r,
				function(error, response, body) {
					if(!error){
						var sc = response.statusCode;
						
						if(sc == 201){		
							resolve(body);	
						}else{
							logger.error(r.method.toUpperCase() + " error: " + response.statusCode);
							logger.error(r.method.toUpperCase() + " error: " + body);
							logger.error(r.method.toUpperCase() + " url: " + url);
								
							
							reject(body);
						}
					}else{
						logger.error("reqPut failed");
						logger.error(error);
						reject(error);
					}
				}
			)
		}
	)
}

function reqPost(url, data, headers){
	var method = 'POST';
	var r = {
			method: method,
			uri: url
	};
	
	if(data !== undefined && data !== null){
			r['body'] = data;
	}
		
	if(headers !== undefined & headers !== null){
		r['headers'] = headers;
	}
	
	return new q.Promise(function(resolve, reject){
			request(
				r,
				function(error, response, body) {
					if(!error){
						var sc = response.statusCode;
						
						if(sc >= 200 && sc < 300){		
							resolve(body);	
						}else{
							logger.error(method.toUpperCase() + " error: " + response.statusCode);
							logger.error(method.toUpperCase() + " error: " + body);
							logger.error(method.toUpperCase() + " url: " + url);
								
							
							reject(body);
						}
					}else{
						logger.error("reqPost failed");
						logger.error(error);
						reject(error);
					}
				}
			)
		}
	)
}

module.exports.qrequest = qrequest
