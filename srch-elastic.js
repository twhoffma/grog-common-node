qrequest = require('./qrequest.js')
logger = require('./logger.js')

c = JSON.parse(fs.readFileSync('localconfig.json', 'utf8'));

var srchURL = "http://" + c.elastic.addr + ":" + c.elastic.port;
var srchIndex = c.elastic.srchIndex;
var srchType = c.elastic.srchType;

/* --- Generic functions --- */
function getSrchURL(){
	return srchURL + "/" + srchIndex + "/" + srchType + "/_search" 
}

//Update search engine
function updateSearch(docs){
	var url = srchURL + "/_bulk"
	var bulk_request = [];
	var p = [];
	var h = {};

	docs.forEach(function(doc){
			var idx;
			if(doc.type === "boardgame"){
				idx = "boardgames";
			}
			
			h = {'update': {"_id": doc._id, "_type": doc.type, "_index": idx}};
			bulk_request.push(JSON.stringify(h));
			bulk_request.push(JSON.stringify({"doc": doc, "doc_as_upsert": true}));
	});
	
	return qrequest.qrequest("POST", url, bulk_request.join("\n") + "\n", {"Content-Type": "application/json"}).then(
		function(v){
			var r = JSON.parse(v);
			var cntCreated = 0;
			var cntUpdated = 0;
			//console.log(r);	
			r.items.forEach(function(i){
				if(i.update.status == 201){
					cntCreated++;
				}else{
					cntUpdated++;
				}
			});
			
			logger.info("[SearchEngine] Created: " + cntCreated + ", Updated: " + cntUpdated);
			
			return v
		},
		function(e){
			logger.error(e);
			throw e
		}
	)
}

//Search by filtering and sort result
function srchBoardgames(geeklistid, filters, sortby, sortby_asc, skip, lim){
	var q = {};
	
	//Number of items to skip during incremental loading
	if(skip != 0){
		q['from'] = skip;	
	}
	
	//Set the number results to return
	q['size'] = lim;
	
	//Filtering
	q['query'] = {};
	q['query']['bool'] = {};
	q['query']['bool']['must'] = [];
	
	q['query']['bool']['must'].push(filterGeeklistId(geeklistid));	
	
	['boardgamedesigner', 'boardgameartist', 'boardgamemechanic', 'boardgamecategory', 'boardgamepublisher', 'boardgamefamily'].forEach(function(e){	
		if(filters[e] != undefined){ 
			q['query']['bool']['must'].push(filterManyToMany(e, filters[e]));
		}
	});

	if(filters["releasetype"] != undefined){
		switch(filters["releasetype"]){
			case 'boardgame':
				//q['query']['bool']['must'].push(filterIsExpansion());
				q['query']['bool']['must'].push(filterIsEmpty('expands'));
				break;
			case 'expansion':
				if(q['query']['bool']['must_not'] === undefined){  
					q['query']['bool']['must_not'] = [];	
				}
				//q['query']['bool']['must_not'].push(filterIsExpansion());
				q['query']['bool']['must_not'].push(filterIsEmpty('expands'));
				break;
			case 'reimplementation':
				if(q['query']['bool']['must_not'] === undefined){  
					q['query']['bool']['must_not'] = [];	
				}
				q['query']['bool']['must_not'].push(filterIsEmpty('boardgameimplementation'));
				break;
			case 'integration':
				if(q['query']['bool']['must_not'] === undefined){  
					q['query']['bool']['must_not'] = [];	
				}
				q['query']['bool']['must_not'].push(filterIsEmpty('boardgameintegration'));
				break;
			case 'collection':
				if(q['query']['bool']['must_not'] === undefined){  
					q['query']['bool']['must_not'] = [];	
				}
				q['query']['bool']['must_not'].push(filterIsEmpty('boardgamecompilation'));
				break;
		}
	}

	//Playing time
	if(filters["playingtimemin"] != undefined || filters["playingtimemax"] != undefined){
		q['query']['bool']['must'].push(filterRange("playingtime", filters["playingtimemin"] || -Infinity, filters["playingtimemax"] || Infinity));
	}
	
	//Number of players
	if(filters["numplayersmin"] != undefined || filters["numplayersmax"] != undefined){
		q['query']['bool']['minimum_should_match'] = 1;
		q['query']['bool']['should'] = [];
		q['query']['bool']['should'].push(filterRange("minplayers", filters["numplayersmin"] || -Infinity, filters["numplayersmax"] || Infinity));
		q['query']['bool']['should'].push(filterRange("maxplayers", filters["numplayersmin"] || -Infinity, filters["numplayersmax"] || Infinity));
	}
		
	//Year published
	if(filters["yearpublishedmin"] != undefined || filters["yearpublishedmax"] != undefined){
		q['query']['bool']['must'].push(filterRange("yearpublished", filters["yearpublishedmin"] || -Infinity, filters["yearpublishedmax"] || Infinity));
	}
	
	//Sorting
	var orderby;
	var s;
	
	if(sortby_asc == 0){
		orderby = "desc";
	}else{
		orderby = "asc";	
	}
		
	q['sort'] = [];
	
    if(sortby === "name"){
		s = {"name.name": {"order": orderby, "nested_filter": {"term": {"name.primary": "true"}}}};	
	}else if(sortby === "yearpublished"){
		s = {"yearpublished": {"order": orderby}}
	}else if(sortby === "thumbs"){
		s = {"geeklists.latest.thumbs": {"order": orderby, "nested_path": "geeklists.latest", "nested_filter": {"term": {"geeklists.latest.geeklistid": geeklistid}}}}
	}else if(sortby === "cnt"){
		s = {"geeklists.latest.cnt": {"order": orderby, "nested_path": "geeklists.latest", "nested_filter": {"term": {"geeklists.latest.geeklistid": geeklistid}}}}	
	}else{
		s = {"geeklists.crets": {"order": orderby, "nested_path": "geeklists", "nested_filter": {"term": {"geeklists.objectid": geeklistid}}}}	
	}
		
	q['sort'].push(s);
	
	var json_query = JSON.stringify(q);
	
	console.log("this is q:\n" + json_query);
	
	return qrequest.qrequest("POST", getSrchURL(), json_query);
}

function filterGeeklistId(geeklistid){
	var q = {};
	q['filtered'] = {};
	q['filtered']['filter'] = {};
	q['filtered']['filter']['nested'] = {};
	q['filtered']['filter']['nested']['path'] = "geeklists";
	q['filtered']['filter']['nested']['filter'] = {};
	q['filtered']['filter']['nested']['filter']['bool'] = {};
	q['filtered']['filter']['nested']['filter']['bool']['must'] = [];
		
	var m = q['filtered']['filter']['nested']['filter']['bool']['must'];
	m.push({'term': {'geeklists.objectid': geeklistid}});

	return q
}

function filterManyToMany(nameM2M, objectid){
	var q = {};
	q['filtered'] = {};
	q['filtered']['filter'] = {};
	q['filtered']['filter']['nested'] = {};
	q['filtered']['filter']['nested']['path'] = nameM2M;
	q['filtered']['filter']['nested']['filter'] = {};
	q['filtered']['filter']['nested']['filter']['bool'] = {};
	q['filtered']['filter']['nested']['filter']['bool']['must'] = [];
		
	var m = q['filtered']['filter']['nested']['filter']['bool']['must'];
	var t = {};
	t['term'] = {};
	t['term'][nameM2M + '.objectid'] = objectid;
	m.push(t);
	//m.push({'term': {nameM2M + '.objectid': objectid}});

	return q
}

function filterIsExpansion(){
	var q = {};
	q['filtered'] = {};
	q['filtered']['filter'] = {};
	q['filtered']['filter']['missing'] = {};
	q['filtered']['filter']['missing']['field'] = 'expands';

	return q
}

function filterIsEmpty(fieldName){
	var q = {};
	q['filtered'] = {};
	q['filtered']['filter'] = {};
	q['filtered']['filter']['missing'] = {};
	q['filtered']['filter']['missing']['field'] = fieldName;

	return q
}

function filterRange(name, min, max){
	var q = {};
	q['filtered'] = {};
	q['filtered']['filter'] = {};
	q['filtered']['filter']['range'] = {};
	q['filtered']['filter']['range'][name] = {};

	if(min > -Infinity){
		q['filtered']['filter']['range'][name]['gte'] = min;
	}

	if(max < Infinity){
		q['filtered']['filter']['range'][name]['lte'] = max;
	}

	return q
}

module.exports.getSrchURL = getSrchURL
module.exports.srchBoardgames = srchBoardgames
module.exports.updateSearch = updateSearch
