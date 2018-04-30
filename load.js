'use strict';

const assert = require('assert');
const fs = require('fs');
const zlib = require('zlib');
const elasticsearch = require('elasticsearch');
const {
	ElasticSearchStream,
	Unpack,
	SimpleModifier
} = require('./libs/es');

assert(process.argv.length === 3, 'help: load.js {file}');

const file = fs.createReadStream(process.argv[2]);
const gunzip = zlib.createUnzip();
const unpack = new Unpack().load(fs.readFileSync('./asset.proto'));
const target = new ElasticSearchStream().setClient(new elasticsearch.Client({
	host: 'localhost:9200'
}));

// just print assets
class Echo extends SimpleModifier {
	constructor(options) {
		super(options);
	}
	_transform(asset, encoding, callback) {
		console.log(asset);
		this.push(asset);
		callback();
	}
}

// remove from asset 2 fields
class Cleaner extends SimpleModifier {
	constructor(options) {
		super(options);
	}
	_transform(asset, encoding, callback) {
		delete asset.source._created_time;
		delete asset.source._updated_time;
		this.push(asset);
		callback();
	}
}

// remove assets with creator = test
class Skipper extends SimpleModifier {
	constructor(options) {
		super(options);
	}
	_transform(asset, encoding, callback) {
		if (asset.source.creator !== 'test') {
			this.push(asset);
		}
		callback();
	}
}

// create from 1 asset - 10 assets in different indices
class Cloner extends SimpleModifier {
	constructor(options) {
		super(options);
	}
	_transform(asset, encoding, callback) {
		const index = asset.index;
		const type = asset.type;
		const description = asset.source.description;
		for (let i = 0; i < 10; i++) {
			asset.index = 'new_' + index + '_' + i;
			asset.type = 'new_' + type + '_' + i;
			asset.source.description = description + ' # ' + i;
			this.push(asset);
		}
		callback();
	}
}

// dont pass any asset to elasticsearch
class Breaker extends SimpleModifier {
	constructor(options) {
		super(options);
	}
	_transform(asset, encoding, callback) {
		callback();
	}
}

file
	.pipe(gunzip)
	.pipe(unpack)
	.pipe(new Echo())
	.pipe(new Cleaner())
	.pipe(new Skipper())
	.pipe(new Cloner())
	.pipe(new Echo())
	.pipe(new Breaker())
	.pipe(target);