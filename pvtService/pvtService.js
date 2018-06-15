var express = require('express');
var config = require('../config/config.json');

var async = require('async');
var log4js = require('log4js');
var logger = log4js.getLogger( "test-file-appender" );
log4js.configure('./config/log4js.json');
var dbService = require('./dbService');
var util = require('./util');

var Q = require('q');

var service = {};

function handleCUD(_params) {
	// Create, Update & Delete
	
	var deferred = Q.defer();
	//params {additions:[], updates:[], deletions:[]}
	async.series({
		additions: function(params, callback) {
			params.additions.forEach(function(doc){
				dbService.insertDocument(params.collection, doc);
				doc.cudStatus = {errors:[], data:[]};
				doc.cudStatus.data.push({status:'successful'});
			});
			callback(null, {status:'successful'});
		}.bind(this, _params),
		updateDBCounter: function(params, callback) {
			if (params.updates.length == 0) {
				callback(null, {status:'successful'});
			} else {
				var _ids = [];
				var ObjectId = require('mongodb').ObjectId;
				
				params.updates.forEach(function(item){
					var o_id = new ObjectId(item._id);
					_ids.push(o_id);
					item.oId = o_id;
				});
				
				var _counterQuery = {
					collection:params.collection,
					query: {
						_id:{$in:_ids}
					}
				};
				
				dbService.getDocument(_counterQuery).then(function(params, result){
					result.forEach(function(ritem){					
						var _matchedUpdate = params.updates.filter(item=>item.oId.equals(ritem._id));
						if (_matchedUpdate.length > 0) {
							_matchedUpdate[0].dbUpdateCounter = ritem.updateCounter;
							if (_matchedUpdate[0].updateCounter != _matchedUpdate[0].dbUpdateCounter) {
								_matchedUpdate[0].cudStatus = {errors:[], data:[]};
								_matchedUpdate[0].cudStatus.errors({errorCode:'', errorMsg:'Update Conflict. Refresh and Update Again'});
							}
						}
					});
					callback(null, {status:'successful'});
				}.bind(this, params), function(err){
					logger.error("updateDBCounter Error: " + JSON.stringify(params.err));
					callback(err, null);
				});
			}			
		}.bind(this, _params),
		updates: function(params, callback) {
			params.updates.forEach(function(doc){
				if (typeof doc.cudStatus === 'undefined') {
					// update document
					var ObjectId = require('mongodb').ObjectId;
					var o_id = new ObjectId(doc._id);
					var _find = {
						_id:o_id						 
					};
					
					var _set = {};
					Object.keys(doc).forEach(function(key){
						if ((key.toUpperCase() === 'OID') || (key.toUpperCase() === '_ID') || (key.toUpperCase() === 'UPDATECOUNTER') || (key.toUpperCase() === 'DBUPDATECOUNTER')) {
							// do nothing
						} else {
							_set[key] = doc[key];
						}
					});
					
					_set.updateCounter = doc.dbUpdateCounter + 1;
					_set.lastUpdate = util.getSysDate();
					
					dbService.updateDocument(params.collection, _find, _set);
					doc.cudStatus = {errors:[], data:[]};
					doc.cudStatus.data.push({status:'successful'});
				}
			});
			callback(null, {status:'successful'});
		}.bind(this, _params),
		deleteDocs: function(params, callback) {
			if (params.deletions.length == 0) {
				callback(null, {status:'successful'});
			} else {
				var _ids = [];
				var ObjectId = require('mongodb').ObjectId;			
				params.deletions.forEach(function(item){
					var o_id = new ObjectId(item);
					_ids.push(o_id);				
				});
				
				var _counterQuery = {
					collection:params.collection,
					query: {					
						_id:{$in:_ids}
					}
				};
				
				dbService.removeDocument(_counterQuery).then(function(params, result){
					params.deletions.forEach(function(doc){
						doc.cudStatus = {errors:[], data:[]};
						doc.cudStatus.data.push({status:'successful'});
					});
				}.bind(this, params));
				callback(null, {status:'successful'});
			}
		}.bind(this, _params)
	}, function(err, result){
		if (err) {
			deferred.reject(err);
		} else {
			deferred.resolve(result);
		}
	});
	
	return deferred.promise;
}
service.handleCUD = handleCUD;

module.exports = service;
