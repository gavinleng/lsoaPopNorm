/*
 * Created by G on 07/10/2016.
 */


"use strict";

var _ = require("lodash");

module.exports = exports = {
	popNorm: function(dataArray, normInf, cb) {
		var _idplus = 'norm';
		var _iddescription = '';
		
		var areaIds = _.map(dataArray, 'area_id');
		areaIds = _.uniq(areaIds);
		
		const output = normInf.output;
		const api = normInf.context.tdxApi;
		
		var mappingId = function (areaIds, cb) {
			output.debug("fetching parent_id data for %s", normInf.mappingId);
			
			var datasetId = normInf.mappingId;
			var key = "parent_id";
			var filter = {"child_id": {"$in": areaIds}, "parent_type": "LAD15CD", "child_type": "LSOA11CD"};
			var projection = null;
			var options = {"limit": 15390120};
			
			api.getDistinct(datasetId, key, filter, projection, options, function (err, response) {
				if (err) {
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
					var options = {"limit": 15390120};
					
					api.getDistinct(datasetId, key, filter, projection, options, function (err, response) {
						if (err) {
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
			
			mappingId(areaIds, function (pid, cids) {
				output.debug("fetching data for %s", normInf.normId);
				
				var datasetId = normInf.normId;
				var filter = {"area_id": pid, "year": yeard, "age_band": {"$ne": "All Ages"}};
				var projection = null;
				var options = {"limit": 15390120};
				
				api.getDatasetData(datasetId, filter, projection, options, function (err, response) {
					if (err) {
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
			var i;
			
			// check data for all areas
			var areaIds = _.map(dataArray, 'area_id');
			areaIds = _.uniq(areaIds);
			
			for (i = 0; i < cids.length; i++) {
				if (areaIds.indexOf(cids[i]) < 0) {
					console.log("All data of each area in this district should be projected and provided for normalizing. Please check it.");
					process.exit(-1);
				}
			}
			
			var ageBands = _.map(dataArray, 'age_band');
			ageBands = _.uniq(ageBands);
			
			var genderNorm = function (gender, ageBandsg) {
				var j, dataAge, dataAllC, _value, _rate;
				
				dataAge = _.filter(dataArray, {'age_band': ageBandsg, 'gender': gender});
				dataAllC = _.filter(dataAll, {'age_band': ageBandsg, 'gender': gender});
				
				_value = 0;
				for (j = 0; j < dataAge.length; j++) {
					_value += dataAge[j].persons;
				}
				
				var datag = [];
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
					
					datag.push(dataAge[j]);
				}
				
				return datag;
			};
			
			var data = [];
			for (i = 0; i < ageBands.length; i++) {
				// for female
				data = data.concat(genderNorm('female', ageBands[i]));
				
				// for male
				data = data.concat(genderNorm('male', ageBands[i]));
			}
			
			var genderAllAges = function (dataAll, gender, year, areaIdsg) {
				var j, dArray, lendArray, dObj, personTatal;
				
				dArray = _.filter(dataAll, {'area_id': areaIdsg, 'gender': gender, 'year': year});
				
				dObj = _.cloneDeep(dArray[0]);
				dObj.age_band = 'All Ages';
				
				lendArray = dArray.length;
				personTatal = 0;
				var dataAllg = [];
				for (j = 0; j < lendArray; j++) {
					personTatal += +dArray[j].persons;
				}
				
				dObj.persons = +personTatal.toFixed(2);
				
				dataAllg.push(dObj);
				
				return dataAllg;
			};
			
			var getAllAges = function (dataAll, cb) {
				var i;
				
				var year = dataAll[0].year;
				
				var areaIds = _.map(dataAll, 'area_id');
				areaIds = _.uniq(areaIds);
				var lenareaIds = areaIds.length;
				
				for (i = 0; i < lenareaIds; i++) {
					// for female
					dataAll = dataAll.concat(genderAllAges(dataAll, 'female', year, areaIds[i]));
					
					// for male
					dataAll = dataAll.concat(genderAllAges(dataAll, 'male', year, areaIds[i]));
				}
				
				cb(dataAll);
			};
			
			getAllAges(data, function (dataAll) {
				cb(dataAll);
			});
		});
	},
	
	popDegroupAgeband: function(projectedData, dataNorm, cb) {
		var _dataYear, _totalPersons;
		
		var _idplus = '-year';
		
		var normYeardata = [];
		
		_.forEach (dataNorm, function (normobj) {
			if ((normobj.age_band != '90+') && (normobj.age_band != 'All Ages')) {
				_dataYear = _.filter(projectedData, function (item) {
					return ((+item.age_band >= +normobj.age_band.split('-')[0]) && (+item.age_band <= +normobj.age_band.split('-')[1]) && (item.year == normobj.year) && (item.gender == normobj.gender) && (item.area_id == normobj.area_id));
					
				});
				
				_totalPersons = 0;
				_.forEach(_dataYear, function (item) {
					_totalPersons += +item.persons;
				});
				
				_.forEach(_dataYear, function (item) {
					item.persons = +(+item.persons * (+normobj.persons) / _totalPersons).toFixed(2);
					
					item.popId = normobj.popId + _idplus;
					
					normYeardata.push(item);
				});
			} else {
				normobj.popId = normobj.popId + _idplus;
				
				normYeardata.push(normobj);
			}
		});
		
		cb(normYeardata);
	}
};
