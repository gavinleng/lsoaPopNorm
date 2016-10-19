/*
 * Created by G on 07/10/2016.
 */


var fs = require("fs");
var path = require("path");

var mappingId = require("mappingid-bot");

//normalized module
var dataNorm = require('./dataNorm.js');

//var popPrjAG = "B1xex1pXR";
//var popPrjAGId = "rJluQRTQR";
//var popONSNorm = "S1gaspIGR";
//var mappingId = "SkxyHi_MR";
/*var year = "2015;
 var areaId = ["E06000045"];
 var outPath = "./SotonNorm2015.json";*/

function databot(input, output, context) {
	output.progress(0);
	
	if (!input.year || !input.areaId || !input.outPath || !input.mappingId || !input.popPrjAGId || !input.popONSNorm || !input.popPrjAG) {
		output.error("invalid arguments - please supply year, areaId, outPath, mappingId, popPrjAGId, popONSNorm, popPrjAG");
		process.exit(1);
	}
	
	var year1 = input.year;
	
	var stringArray = input.areaId;
	
	const outPath = path.resolve(input.outPath);
	
	var p2c_config = {"mappingId": input.mappingId, "parent_type": "LAD15CD", "child_type": "LSOA11CD", "dataId": stringArray, "context": context, "output": output};
	
	mappingId.p2cId(p2c_config, function (array) {
		output.debug("fetching data for %s", input.popPrjAGId);
		
		const api = context.tdxApi;
		
		var datasetId = input.popPrjAGId;
		var filter = {"area_id":{"$in": array},"year": year1,"age_band":{"$ne":"All Ages"}};
		var projection = null;
		var options = {"limit":15390120};
		
		api.getDatasetData(datasetId, filter, projection, options, function(err, response) {
			if(err) {
				output.error("Failed to get data - %s", err.message);
				process.exit(1);
			} else {
				output.debug("got data");
				
				var dataArray = response.data;
				
				datasetId = input.popPrjId;
				filter = {"area_id":{"$in": array},"year": year1};
				projection = null;
				options = {"limit":15390120};
				
				api.getDatasetData(datasetId, filter, projection, options, function(err, response) {
					if(err) {
						output.error("Failed to get data - %s", err.message);
						process.exit(1);
					} else {
						output.debug("got data");
						
						var projectedData = response.data;
						
						var normInf = {
							"normId": input.popONSNorm,
							"output": output,
							"context": context,
							"mappingId": input.mappingId
						};
						
						dataNorm(projectedData, dataArray, normInf, function (data) {
							var i;
							var lendata = data.length;
							
							output.debug("writing file %s", outPath);
							
							var stream = fs.createWriteStream(outPath);
							for (i = 0; i < lendata; i++) {
								stream.write(JSON.stringify(data[i]) + "\n");
							}
							stream.end();
						});
					});
			}
		});
	});
}

var input = require("nqm-databot-utils").input;
input.pipe(databot);
