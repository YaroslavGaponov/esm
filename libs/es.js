'use strict';

const assert = require('assert');
const stream = require('stream');
const protobuf = require('protocol-buffers');

class ElasticSearchStream extends stream.Duplex {

	constructor(options) {

		options = options || {};
		options.objectMode = true;
		super(options);

		this._client = null;
		this._index = null;
		this._type = null;

		this._counter = 0;
		this._isFinish = false;
	}


	setClient(client) {
		assert(client);
		this._client = client;
		return this;
	}

	setIndex(index) {
		assert(index);
		this._index = index;
		return this;
	}

	setType(type) {
		assert(type);
		this._type = type;
		return this;
	}

	_read() {
		assert(this._client);
		assert(this._index);
		assert(this._type);

		if (this._isFinish) return;

		const next = (error, response) => {
			if (error) {
				this.isFinish = true;
				return process.nextTick(() => this.emit('error', error));
			}
			response.hits.hits.forEach(hit => {
				this.push({
					index: this._index,
					type: this._type,
					source: hit._source
				});
			});
			this._counter += response.hits.hits.length;
			if (this.counter < response.hits.total) {
				this._client.scroll({
					scrollId: response._scroll_id,
					scroll: '30m'
				}, next);
			} else {
				this._isFinish = true;
				this.emit('end');
			}
		};

		this._client.search({
			index: this._index,
			type: this._type,
			scroll: '30m',
			q: '*'
		}, next);

	}

	_write(asset, encoding, callback) {
		assert(this._client);
		assert(asset);

		this._client.create({
				index: asset.index,
				type: asset.type,
				id: asset.source.id,
				body: asset.source
			},
			(error, response) => {
				if (error) {
					return process.nextTick(() => this.emit('error', error));
				}
				return callback(null, response);
			});
	}

	static createProto(client, index, type) {
		assert(client);
		assert(index);
		assert(type);

		return client.indices.getMapping({
				index: index,
				type: type
			})
			.then(mapping => {

				const getMessageName = field => field[0].toUpperCase() + field.substring(1);

				const getType = (type) => {
					switch (type) {
						case 'string':
							return 'string';
						case 'date':
							return 'string';
						case 'geo_point':
							return 'string';
						case 'long':
							return 'sint64';
						case 'boolean':
							return 'bool';
					}
					assert(false, `Type ${type} is not supported`);
				};

				const getMessage = (name, mapping) => {
					let message = [];
					message.push(`message ${name} {`);
					let index = 1;
					for (let field in mapping) {
						if ('properties' in mapping[field] || mapping[field].type === 'object') {
							const subMessageName = getMessageName(field);
							const cardinality = (mapping[field].type === 'nested') || field.endsWith('s') ? 'repeated' : 'optional';
							message.push(`\t${cardinality} ${subMessageName} ${field} = ${index++};`);
							message = getMessage(subMessageName, mapping[field].properties).concat(message);
						} else {
							const cardinality = field.endsWith('s') ? 'repeated' : 'optional';
							message.push(`\t${cardinality} ${getType(mapping[field].type)} ${field} = ${index++};`);
						}
					}
					message.push('}');
					return message;
				};

				const messages = getMessage(
					getMessageName(type),
					mapping[index].mappings[type].properties
				);

				messages.push('message EntryPoint {');
				messages.push('\trequired string index = 1;');
				messages.push('\trequired string type = 2;');
				messages.push(`\trequired ${getMessageName(type)} source = 3;`);
				messages.push('}');

				return messages.join('\n');
			});
	}
}

class Pack extends stream.Transform {
	constructor(options) {
		options = options || {};
		options.readableObjectMode = true;
		options.writableObjectMode = true;
		super(options);

		this.schema = null;

	}
	load(proto) {
		assert(proto);
		this.schema = protobuf(proto);
		return this;
	}
	_transform(chunk, encoding, callback) {
		assert(this.schema);
		const buf = this.schema.EntryPoint.encode(chunk);
		callback(null, buf);
	}
}

class Unpack extends stream.Transform {
	constructor(options) {
		options = options || {};
		options.readableObjectMode = true;
		options.writableObjectMode = true;
		super(options);

		this.schema = null;

	}
	load(proto) {
		assert(proto);
		this.schema = protobuf(proto);
		return this;
	}
	_transform(chunk, encoding, callback) {
		assert(this.schema);
		const obj = this.schema.EntryPoint.decode(chunk);
		callback(null, obj);
	}
}

class SimpleModifier extends stream.Transform {
	constructor(options) {
		options = options || {};
		options.readableObjectMode = true;
		options.writableObjectMode = true;
		super(options);
	}
	_transform(asset, encoding, callback) {
		this.push(asset);
		callback();
	}
}


module.exports = {
	ElasticSearchStream,
	Pack,
	Unpack,
	SimpleModifier
};