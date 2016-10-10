/*
 * Created by G on 11/07/2016.
 */


"use strict";

module.exports = exports = {
	_getData: function (config, cb) {
		var dataId = config.dataId;

		const output = config.output;
		const api = config.context.tdxApi;
		
		output.debug("fetching data for %s", config.mappingId);
		
		var datasetId = config.mappingId;
		var key = config.vId;
		var filter = {"parent_type": config.parent_type, "child_type": config.child_type};
		filter[config.fId] = {"$in": dataId};
		var projection = null;
		var options = {"limit":15390120};
		
		api.getDistinct(datasetId, key, filter, projection, options, function(err, response) {
			if(err) {
				output.error("Failed to get data - %s", err.message);
				process.exit(1);
			} else {
				output.debug("got data");
				output.progress(50);
				
				var dataGet = response.data;

				cb(dataGet);
			}
		});
	},
	
	p2cId: function(config, cb) {
		config.fId = "parent_id";
		config.vId = "child_id";

		this._getData(config, function (data) {
			cb(data);
		});
	},

	c2pId: function(config, cb) {
		config.fId = "child_id";
		config.vId = "parent_id";

		this._getData(config, function (data) {
			cb(data);
		});
	}
};
