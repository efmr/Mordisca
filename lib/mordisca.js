var _ = require('lodash');

var mordisca = {};
mordisca.Client = require('./client');
mordisca.CacheLayer = require('./cachelayer');
mordisca.Collection = require('./collection');
mordisca.EJSON = require('extended-json');
mordisca.ObjectID = require('mongodb').ObjectID;
mordisca.Subscriber = require('./subscriber');
mordisca.Saver = require('./saver');
mordisca.Transaction = require('./transaction');
_.merge(mordisca, require('./structure'));
mordisca.utils = require('./utils');

module.exports = mordisca;
