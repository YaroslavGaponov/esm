'use strict';

const assert = require('assert');
const fs = require('fs');
const zlib = require('zlib');
const elasticsearch = require('elasticsearch');
const {
	ElasticSearchStream,
	Pack,
	SimpleModifier
} = require('./libs/es');

assert(process.argv.length === 3, 'help: save.js {file}');

const source = new ElasticSearchStream()
	.setClient(new elasticsearch.Client({
		host: 'localhost:9200'
	}))
	.setIndex('index')
	.setType('type');

const pack = new Pack().load(fs.readFileSync('./asset.proto'));
const gzip = zlib.Gzip();
const file = fs.createWriteStream(process.argv[2]);

// print statistics
class Echo extends SimpleModifier {
	constructor(options) {
		super(options);
		this._count = 0;
	}
	_transform(asset, encoding, callback) {
		console.log('%s assets', ++this._count);
		this.push(asset);
		callback();
	}
}

// modify asset id
class ReIndexer extends SimpleModifier {
	constructor(options) {
		super(options);
	}
	_transform(asset, encoding, callback) {
		const id = asset.source.id;
		asset.source.id = 'new_' + id;
		console.log('Old ID = %s, new Id = %s', id, asset.source.id);
		this.push(asset);
		callback();
	}
}

source
	.pipe(new Echo())
	.pipe(new ReIndexer())
	.pipe(pack)
	.pipe(gzip)
	.pipe(file);