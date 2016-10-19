/*
 * Created by G on 07/10/2016.
 */


"use strict";

var popNorm= require('./lib/popNorm.js');

module.exports = exports =  function(projectedData, dataArray, normInf, cb) {
    popNorm.popNorm(dataArray, normInf, function(dataNorm) {
        popNorm.popDegroupAgeband(projectedData, dataNorm, function (data) {
            cb(data);
        });
    });
};
