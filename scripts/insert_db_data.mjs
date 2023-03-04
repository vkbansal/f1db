import 'dotenv/config';

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { MongoClient, ServerApiVersion } from 'mongodb';
import Papa from 'papaparse';
import { SingleBar, Presets } from 'cli-progress';

import { Indexes } from './utils.mjs';

const BATCH_SIZE = 50;
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DB_PATH = path.resolve(process.cwd(), 'f1db');
const files = await fs.promises.readdir(DB_PATH);

const mongoClient = new MongoClient(process.env.MONGO_DB_URL, {
	serverApi: ServerApiVersion.v1,
});

await mongoClient.connect();

const db = mongoClient.db(process.env.MONGO_DB_NAME);
const bar = new SingleBar({}, Presets.shades_classic);
const SKIP = ['lap_times'];

for (const file of files) {
	const content = await fs.promises.readFile(path.resolve(DB_PATH, file), 'utf8');
	const collection_name = file.replace(path.extname(file), '');

	if (SKIP.includes(collection_name)) {
		continue;
	}

	const collection = db.collection(collection_name);

	const { data, errors } = Papa.parse(content.trim(), {
		header: true,
		fastMode: false,
		dynamicTyping: (header) => {
			if (header.endsWith('Text')) {
				return false;
			}

			return true;
		},
		transform(value) {
			if (value === '\\N') {
				return null;
			}

			return value;
		},
	});

	if (errors.length > 0) {
		console.log(errors);
		process.exit(1);
	}

	await collection.drop();

	let start = 0;

	console.log(`Inserting collection: ${collection_name}`);
	bar.start(data.length, 0);

	while (start < data.length) {
		const next = start + BATCH_SIZE;
		const batchData = data.slice(start, next);

		await collection.insertMany(batchData);
		bar.update(next);

		start = next;
	}

	bar.stop();

	console.log(`Creating indexes...`);

	const indexes = Indexes[collection_name];

	for (const params of indexes) {
		await collection.createIndex(...params);
	}
}

await mongoClient.close(true);
