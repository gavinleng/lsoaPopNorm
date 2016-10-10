/*
 * Created by G on 07/10/2016.
 */


"use strict";

var _ = require("lodash");

module.exports = exports = function(dataArray, normInf, cb) {
	var _idplus = 'norm';
	var _iddescription = '';
	
	var areaIds = _.pluck(dataArray, 'area_id');
	areaIds = _.uniq(areaIds);
	
	const output = normInf.output;
	const api = normInf.context.tdxApi;
	
	var mappingId = function(areaIds, cb) {
		output.debug("fetching parent_id data for %s", normInf.mappingId);
		
		var datasetId = normInf.mappingId;
		var key = "parent_id";
		var filter = {"child_id":{"$in": areaIds}, "parent_type": "LAD15CD", "child_type": "LSOA11CD"};
		var projection = null;
		var options = {"limit":15390120};
		
		api.getDistinct(datasetId, key, filter, projection, options, function(err, response) {
			if(err) {
				output.error("Failed to get data - %s", err.message);
				process.exit(1);
			} else {
				output.debug("got data");
				output.progress(50);
				
				var data = response.data;
				
				if (data.length != 1) {
					console.log("All data should be in one district. Please check it.");
					process.exit(-1);
				}
				
				output.debug("fetching child_id data for %s", normInf.mappingId);
				
				var datasetId = normInf.mappingId;
				var key = "child_id";
				var filter = {"parent_id": data[0], "parent_type": "LAD15CD", "child_type": "LSOA11CD"};
				var projection = null;
				var options = {"limit":15390120};
				
				api.getDistinct(datasetId, key, filter, projection, options, function(err, response) {
					if(err) {
						output.error("Failed to get data - %s", err.message);
						process.exit(1);
					} else {
						output.debug("got data");
						output.progress(50);
						
						var data1 = response.data;
						
						cb(data[0], data1);
					}
				});
			}
		});
	};
	
	
	var dataDistrict = function (areaIds, dataArray, normInf, cb) {
		var yeard = dataArray[0].year;
		
		mappingId (areaIds, function (pid, cids) {
			output.debug("fetching data for %s", normInf.normId);
			
			var datasetId = normInf.normId;
			var filter = {"area_id": pid,"year": yeard,"age_band":{"$ne":"All Ages"}};
			var projection = null;
			var options = {"limit":15390120};
			
			api.getDatasetData(datasetId, filter, projection, options, function(err, response) {
				if(err) {
					output.error("Failed to get data - %s", err.message);
					process.exit(1);
				} else {
					output.debug("got data");
					output.progress(50);
					
					var dataAll = response.data;
					
					cb(dataAll, cids);
				}
			});
		});
	};
	
	dataDistrict(areaIds, dataArray, normInf, function (dataAll, cids) {
		var i, j, dataAge, dataAllC, _value, _rate;
		
		// check data for all areas
		var areaIds = _.pluck(dataArray, 'area_id');
		areaIds = _.uniq(areaIds);
		
		for (i = 0; i < cids.length; i++) {
			if (areaIds.indexOf(cids[i]) < 0) {
				console.log("All data of each area in this district should be projected and provided for normalizing. Please check it.");
				process.exit(-1);
			}
		}
		
		var ageBands = _.pluck(dataArray, 'age_band');
		ageBands = _.uniq(ageBands);
		
		var data = [];
		for (i = 0; i < ageBands.length; i++) {
			// for female
			dataAge =  _.filter(dataArray, {'age_band':ageBands[i], 'gender':'female'});
			dataAllC = _.filter(dataAll, {'age_band':ageBands[i], 'gender':'female'});
			
			_value = 0;
			for (j = 0; j < dataAge.length; j++) {
				_value += dataAge[j].persons;
			}
			
			for (j = 0; j < dataAge.length; j++) {
				if (_value == 0) {
					_rate = _value;
				} else {
					_rate = dataAge[j].persons / _value;
				}
				
				//dataAge[j].rate = +_rate;
				dataAge[j].persons = +(_rate * dataAllC[0].persons).toFixed(2);
				dataAge[j].popId = 'NQM-LSOA-Pop-' + _idplus;
				dataAge[j].popId_description = _iddescription;
				
				data.push(dataAge[j]);
			}
			
			// for male
			dataAge =  _.filter(dataArray, {'age_band':ageBands[i], 'gender':'male'});
			dataAllC = _.filter(dataAll, {'age_band':ageBands[i], 'gender':'male'});
			
			_value = 0;
			for (j = 0; j < dataAge.length; j++) {
				_value += dataAge[j].persons;
			}
			
			for (j = 0; j < dataAge.length; j++) {
				if (_value == 0) {
					_rate = _value;
				} else {
					_rate = dataAge[j].persons / _value;
				}
				
				//dataAge[j].rate = +_rate;
				dataAge[j].persons = +(_rate * dataAllC[0].persons).toFixed(2);
				dataAge[j].popId = 'NQM-LSOA-Pop-' + _idplus;
				dataAge[j].popId_description = _iddescription;
				
				data.push(dataAge[j]);
			}
		}
		
		var getAllAges = function (dataAll, cb) {
			var i, j, dArray, lendArray, dObj, personTatal;
			
			var year = dataAll[0].year;
			
			var areaIds = _.pluck(dataAll, 'area_id');
			areaIds = _.uniq(areaIds);
			var lenareaIds = areaIds.length;
			
			for (i = 0; i <lenareaIds; i++) {
				// for female
				dArray = _.filter(dataAll, {'area_id':areaIds[i], 'gender':'female', 'year':year});
				
				dObj =  _.cloneDeep(dArray[0]);
				dObj.age_band = 'All Ages';
				
				lendArray = dArray.length;
				personTatal = 0;
				for (j = 0; j < lendArray; j++) {
					personTatal += +dArray[j].persons;
				}
				
				dObj.persons = +personTatal.toFixed(2);
				
				dataAll.push(dObj);
				
				// for male
				dArray = _.filter(dataAll, {'area_id':areaIds[i], 'gender':'male', 'year':year});
				
				dObj =  _.cloneDeep(dArray[0]);
				dObj.age_band = 'All Ages';
				
				lendArray = dArray.length;
				personTatal = 0;
				for (j = 0; j < lendArray; j++) {
					personTatal += +dArray[j].persons;
				}
				
				dObj.persons = +personTatal.toFixed(2);
				
				dataAll.push(dObj);
			}
			
			cb(dataAll);
		};
		
		getAllAges(data, function (dataAll) {
			cb(dataAll);
		});
	});
};
