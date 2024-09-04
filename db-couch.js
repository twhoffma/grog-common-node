qrequest = require('./qrequest.js');
logger = require('./logger.js');
fs = require('fs');
c = JSON.parse(fs.readFileSync('localconfig.json', 'utf8'));
	
var dbURL = "http://" + c.db.dbaddr + ":" + c.db.dbport;
var dbName = c.db.dbname;


// --- Generic functions --- 
function getViewURL(dsndoc, view){
	return dbURL + '/' + dbName + '/_design/' + dsndoc + '/_view/' + view
}

function getUpdateURL(dsndoc, view){
	return dbURL + '/' + dbName + '/_design/' + dsndoc + '/_update/' + view
}

function getCompactURL(){
	return dbURL + '/' + dbName + '/_compact'
}

function generateUuids(num){
		var p = [];
		num = parseInt(num);
			
		while(num > 0){
			//XXX: In reality, this can only fetch 1000 ids at a time - so this needs to be patched.
			var idURL = dbURL + '/_uuids?count=' + Math.min(num, 1000);
				
			p.push(
				qrequest.qrequest("GET", idURL, null, null).then(
					function(ret){
						return JSON.parse(ret).uuids
					}).catch(
						function(res){
							throw res
						}
					)
			);
				
			num -= Math.min(1000, num);
		}
		
		return q.all(p).then(function(v){return v.reduce(function(prev, curr){ return prev.concat(curr)})}, [])
}

function deleteDocs(url){
	var p = [];
	
	return getDocs(url).then(
		function(rows){
			var docs = [];

			rows.forEach(function(row, i){
				docs.push({uuid: row.doc._id, rev: row.doc._rev});
			});
			
			return docs
		}
	).then(
		function(docs){
			var p = [];
			
			docs.forEach(
				function(doc, i){
					var url = dbURL + '/' + dbName + '/' + doc.uuid + '?rev=' + doc.rev;
					p.push(qrequest.qrequest("DELETE", url, null, null));
				}
			);

			return q.allSettled(p).then(
				function(success){
					return true
				},
				function(err){
					console.log(err);
					
					throw err
				}
			)
		}
	).catch(function(e){
		console.log(e);
	});	
}

function getDocs(viewURL){
	return qrequest.qrequest("GET", viewURL, null, null).then(
		function(val){
			var data = JSON.parse(val);
			
			logger.debug("getDocs: " + data.rows.length)
			return data.rows
		},
		function(){
			logger.error("No doc found for " + viewURL);
		}
	).catch(
		function(e){
			logger.error(e);
		}
	);
}

function getDoc(viewURL){
	return getDocs(viewURL).then(
		function(d){
			if(d.length > 0){
				return q(d[0].doc);
			}else{
				return q.reject("No doc found!");
			}
		}
	)
}

function saveDocs(docs){
	return generateUuids(docs.length).then(
		function(uuids){
			var promises = [];
			docs.forEach(function(doc, i){
					var docId;
					
					if(doc._id !== undefined){
						docId = doc._id;
					}else if(doc.type === "boardgame" || doc.type === "geeklist"){
						//These are proper things at BGG, so they get to keep their id.
						docId = doc.objectid;
					}else{
						docId = uuids.pop();
					}
					
					var docURL = dbURL + "/" + dbName + "/" + docId;
					
					promises.push(qrequest.qrequest("PUT", docURL, JSON.stringify(doc)).then(
							function(res){
								var reply = JSON.parse(res);
								
								if(reply.ok){
									doc["_id"] = reply.id;
									doc["_rev"] = reply.rev;
									
									return true
								}else{
									throw "DB failed to save"
								}
							}
						).catch(
							function(e){
								logger.error("Failed to save doc: " + doc._id);
								console.log("Failed doc: " + JSON.stringify(doc));
								
								throw e
							}
						)
					);
				}
			);

			return q.all(promises)
		},
		function(err){
			console.log("No uuids");
		}
	).catch(
		function(err){
			console.log(err);
		}
	)	
}

//TODO: Dummy function
function updateDocs(urls, docs){
	var promises = [];
				
	docs.forEach(function(doc, i){
		var url = urls[i];
		//console.log(url);
		
		promises.push(qrequest.qrequest("PUT", url, JSON.stringify(doc)).then(
				function(res){
					var reply = JSON.parse(res);
					
					if(reply.ok){
						return true
					}else{
						console.log(reply);
						throw "DB failed to save"
					}
				}
			).catch(
				function(e){
					logger.error("Failed to update doc: " + doc.objectid);
					
					throw e
				}
			)
		);
	});
	
	//<database>/_design/<design>/_update/<function>/<docid>
	return q.all(promises)
}

function finalizeDb(){
	var url = getCompactURL();
	
	return qrequest.qrequest("POST", url, null, {"Content-Type": "application/json"})
}

module.exports.getViewURL = getViewURL
module.exports.getUpdateURL = getUpdateURL
module.exports.getCompactURL = getCompactURL

module.exports.deleteDocs = deleteDocs
module.exports.getDocs = getDocs
module.exports.getDoc = getDoc
module.exports.saveDocs = saveDocs
module.exports.updateDocs = updateDocs

module.exports.finalizeDb = finalizeDb

