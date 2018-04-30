'use strict';

const elasticsearch = require('elasticsearch');
const {
	ElasticSearchStream
} = require('./libs/es');

ElasticSearchStream.createProto(
	new elasticsearch.Client({
		host: 'localhost:9200'
	}),
	'index',
	'type'
).then(proto => console.log(proto)).catch(err => console.log(err));