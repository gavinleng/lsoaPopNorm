/*
 * Created by G on 07/10/2016.
 */


"use strict";

var popNorm= require('./lib/popNorm.js');

module.exports = exports =  function(dataArray, normInf, cb) {
    popNorm(dataArray, normInf, function(data) {
        cb(data);
    });
};
