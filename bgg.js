cheerio = require('cheerio');
qrequest = require('./qrequest.js');
q = require('q');
parseString = require('xml2js').parseString;
moment = require('moment');

var c = JSON.parse(fs.readFileSync('localconfig.json', 'utf8'));

var geeklistURL = 'https://www.boardgamegeek.com/xmlapi/geeklist/';
var boardgameURL = 'https://www.boardgamegeek.com/xmlapi/boardgame/';

var queueGeeklists = [];
var queueBoardgames = [];

var fetchingGeeklist = false;
var fetchingBoardgame = false;

function Boardgame(boardgameId){
	this.type = "boardgame";
	this.objectid = boardgameId;
	this.yearpublished = 0;
	this.minplayers = 0;
	this.maxplayers = 0;
	this.playingtime = 0;
	this.minplaytime = 0;
	this.maxplaytime = 0;
	this.thumbnail = "";
	this.geeklists = [];
	this.name = [];
	this.boardgamecategory = [];
	this.boardgamemechanic = [];	
	this.boardgamedesigner = [];	
	this.boardgameartist = [];
	this.boardgamefamily = [];
	this.boardgamepublisher = [];
	this.boardgameintegration = [];
	this.boardgameimplementation = [];
	this.boardgamecompilation = [];
	this.expansions = [];
	this.expands = [];
}

function getGeeklist(listtype, geeklistId){
	//Look up a geeklist - use queueing
	logger.info("Getting geeklist " + geeklistId + " of type " + listtype);
	
	if(listtype === "preview"){
		return qrequest.qrequest("GET", `https://api.geekdo.com/api/geekpreviews?nosession=1&previewid=${geeklistId}`, null, null, true, 0, true).then(
			function(results){
				let r = JSON.parse(results);
				let p = [];
				
				for(let i = 1; i <=r.config.numpages; i++){
					p.push(
						qrequest.qrequest("GET", `https://api.geekdo.com/api/geekpreviewitems?nosession=1&pageid=${i}&previewid=${geeklistId}`, null, null, true, 0, true).then(
							function(convPreview){
								return JSON.parse(convPreview).map(
									function(x){
										return	{
											'id': x.itemid, 
											'objecttype':x.objecttype, 
											'subtype': 'boardgame', 
											'objectid': parseInt(x.geekitem.item.objectid || x.objectid), 
											//'objectid': parseInt(x.geekitem.item.objectid), 
											//'objectid': parseInt(x.objectid), 
											'objectname':x.geekitem.item.primaryname.name, 
											'username':'',
											//'postdate': new Date(Date.parse(x.date_created)).toISOString(),
											//'editdate': new Date(Date.parse(x.date_updated)).toISOString(),
											'postdate': moment(x.date_created).toDate().toISOString(),
											'editdate': x.date_updated ? moment(x.date_updated).toDate().toISOString() : moment(x.date_created).toDate().toISOString(),
											'thumbs': parseInt(x.reactions.thumbs),
											'imageid': parseInt(x.geekitem.item.imageid),
											'wants': parseInt(x.stats.interested || 0) + parseInt(x.stats.musthave || 0)
										}
									}
								)
							}
						).catch(function(e){
								return q.reject(e)
							}
						)
					);
				}

				return q.allSettled(p);
			}).then(
				function(r){
					//console.log(r);
					let l = r.filter(function(e){return (e.state === "fulfilled")}).map(e => e.value).reduce(
						function(prev, curr){
							if(prev.indexOf(curr) === -1){
								return prev.concat(curr)
							}else{
								return prev
							}
						},
						[]
					);
					//console.log(l)		
					return l	
				}
			)
	}else if(listtype === "geeklist"){
		//New possibility: 
		//First one can read number of items, then spawn enough page requests to load the entire list. Depends on the size of the list.
		function parseBGGXML(results){
			let n = function(value,name){
        			if(['objectid','thumbs','id','imageid'].includes(name)){
               	 			return parseInt(value)
        			}else if(['postdate','editdate'].includes(name)){
                			//return new Date(Date.parse(value)).toISOString()
                			return new Date(moment(value).toDate()).toISOString()
        			}else{
                			return value
        			}
			}
			
			//FIXME: Appearantly returns an empty Boardgame even if parsing fails.. Need validation.
			return q(new Promise(function(resolve, reject){
				parseString(results, {attrValueProcessors: [n]}, 
					function(err, res){
						if(err){
							return reject(err);
						}else{
							return resolve(res);
						}
					}
				)
			})).then(function(res){
				if(res.error){
					//logger.error(res.error['$'].message);
					return q.reject(res.error['$'].message);
				}
					
				logger.debug(res.geeklist.numitems + " items");
				return q(res);
			});
		}
		
		function getBGGItems(res){
			var items = [];
			
			if(res.geeklist === undefined){
				logger.error("res.geeklist undefined 2!");
				console.log(res);
			}
				
			if(parseInt(res.geeklist.numitems) === 0){
				//console.log(res);
				logger.error("List " + res.geeklist['$'].id + " is empty!");
			}else{
				items = res.geeklist.item.map(x => x['$']);
			}
			
            		return items
		}

		//https://boardgamegeek.com/xmlapi2/geeklist/228286?comments=0&page=1&pagesize=1000
		let pagesize = c.bgg.xmlapi_geeklist_pagesize;
		let url = geeklistURL + geeklistId + `&comments=0&page=1`
			
		if(pagesize > 0){
			url = url + `&pagesize=${pagesize}`
		}
		
		return qrequest.qrequest("GET", url, null, null, true, 0, true).then(
			function(results){
				var items = [];
				//XXX: Problem here with returning results ends the chain..
				return parseBGGXML(results).then(
					function(res){
						let pageItems = getBGGItems(res);
						let totPageItems = pageItems.length;
						
						const filterBgGl = (x => x.filter(x => ((x.objecttype === 'thing' && x.subtype === 'boardgame') || x.objecttype === 'geeklist')));
	
						//Filter out geeklists and boardgames	
						//pageItems = pageItems.filter(x => ((x.objecttype === 'thing' && x.subtype === 'boardgame') || x.objecttype === 'geeklist'));
						pageItems = filterBgGl(pageItems);
            					let numItems = parseInt(res.geeklist.numitems);
						
						let numpages = (pagesize === 0 ? 1 : (parseInt(numItems / pagesize) + 1));
						logger.debug("Need to fetch " + numpages + " pages for " + geeklistId);
						items = items.concat(pageItems);
						
						if(numpages > 1 && numItems === totPageItems){
							logger.debug("Got all items in first query - pagesize ignored by server!");
							numpages = 1;
						}else{
							logger.debug("Got " + pageItems.length + " out of " + numItems + "on first fetch");
						}
						
						let p = [];
			            		//Start on page two, since we've already fetched the first one..
						for(let i = 2; i <= numpages; i++){
							logger.info(`Queued page request #${i}`);
              						let url = geeklistURL + geeklistId + `&comments=0&page=${i}&pagesize=${pagesize}`;

							p.push(
					             		qrequest.qrequest("GET", url, null, null, true, 0, true).then(
				        				r => parseBGGXML(r).then(rr => filterBgGl(getBGGItems(rr)))
                						)
              						);
						}
					    
			                        if(p.length > 0){	
                        				return q.allSettled(p).then(
	    							function(r){
                         						let l = r.filter(e => (e.state === "fulfilled")).map(e => e.value).reduce(
				                               			function(prev, curr){
                            							    	if(prev.indexOf(curr) === -1){
				                                	    			return prev.concat(curr)
                            								}else{
					                                	    		return prev
                            							    	}
				                        			},
                        							items
                    			    				);

								    	return l
                			    			}
						    	)
                        			}else{
			                            	return items
                        			}   
					}
				).catch(function(e){
					return q.reject("Geeklistid=" + geeklistId + ": " + e)
				})
			}
		);
	}else{
		logger.error("Unknown geeklisttype: " + listtype);
	}
}

function getBoardgame(boardgameId){
	//Look up board games - use queuing.
	return qrequest.qrequest("GET", boardgameURL + boardgameId, null, null, true, 0, true).then(
		function(val){
			var $ = cheerio.load(val);
			var boardgames = [];
				
			$('boardgame').each(function(index, elem){
				if($('error', $(this)).length > 0){
					logger.error("BGG returned error for boardgameId(s) " + boardgameId + ": " + ($('error', $(this)).attr('message')));
				}else{
					var bg = new Boardgame($(this).attr('objectid'));
				
					bg.yearpublished = $('yearpublished', $(this)).text();
					bg.minplayers = $('minplayers', $(this)).text();
					bg.maxplayers = $('maxplayers', $(this)).text();
					bg.playingtime = $('playingtime', $(this)).text();
					bg.minplaytime = $('minplaytime', $(this)).text();
					bg.maxplaytime = $('maxplaytime', $(this)).text();
					bg.thumbnail = $('thumbnail', $(this)).text();
					
					$('name', $(this)).each(function(index, elem){
						bg.name.push({'name': $(this).text(), 'primary': $(this).attr('primary')});
					});
			
					$('boardgamecategory', $(this)).each(function(index, elem){
						var id = $(this).attr('objectid');
						var val = $(this).text();
						
						bg.boardgamecategory.push({objectid: id, name: val});
					});
				
					$('boardgamemechanic', $(this)).each(function(index, elem){
						var id = $(this).attr('objectid');
						var val = $(this).text();
						bg.boardgamemechanic.push({objectid: id, name: val});
					});
					
					$('boardgamedesigner', $(this)).each(function(index, elem){
						var id = $(this).attr('objectid');
						var val = $(this).text();
						bg.boardgamedesigner.push({objectid: id, name: val});
					});
			
					$('boardgamefamily', $(this)).each(function(index, elem){
						var id = $(this).attr('objectid');
						var val = $(this).text();
						bg.boardgamefamily.push({objectid: id, name: val});
					});
					
					$('boardgameartist', $(this)).each(function(index, elem){
						var id = $(this).attr('objectid');
						var val = $(this).text();
						bg.boardgameartist.push({objectid: id, name: val});
					});
			
					$('boardgamepublisher', $(this)).each(function(index, elem){
						var id = $(this).attr('objectid');
						var val = $(this).text();
						bg.boardgamepublisher.push({objectid: id, name: val});
					});
					
					$('boardgameintegration', $(this)).each(function(index, elem){
						var id = $(this).attr('objectid');
						var val = $(this).text();
						
						if($(this).attr('inbound') === "true"){
							bg.boardgameintegration.push({objectid: id, name: val});
						}
					});
					
					$('boardgameimplementation', $(this)).each(function(index, elem){
						var id = $(this).attr('objectid');
						var val = $(this).text();
						
						if($(this).attr('inbound') === "true"){
							bg.boardgameimplementation.push({objectid: id, name: val});
						}
					});
					
					$('boardgameexpansion', $(this)).each(function(index, elem){
						var id = $(this).attr('objectid');
						var val = $(this).text();
						
						if($(this).attr('inbound') === "true"){
							bg.expands.push({objectid: id, name: val});
						}else{
							bg.expansions.push({objectid: id, name: val});
						}
					});
					
					$('boardgamecompilation', $(this)).each(function(index, elem){
						var id = $(this).attr('objectid');
						var val = $(this).text();
						
						if($(this).attr('inbound') === "true"){
							bg.boardgamecompilation.push({objectid: id, name: val});
						}
					});
					
					boardgames.push(bg);
				}
			});	
			
			return boardgames
		}
	)
}

function selectorToArray(selector, checkInbound){
	var a = [];
	
	selector.each(function(index, elem){
		console.log(elem);
		var id = this.attr('objectid');
		var val = this.text();
		var inbound = this.attr('inbound');
				
		if(!checkInbound || inbound === "true"){
			a.push({objectid: id, name: val});
		}
	});

	return a
}

module.exports.getGeeklist = getGeeklist
module.exports.getBoardgame = getBoardgame
