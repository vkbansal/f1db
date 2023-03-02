import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { MongoClient, ServerApiVersion } from 'mongodb';
import Papa from 'papaparse';

const BATCH_SIZE = 50;
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DB_PATH = path.resolve(process.cwd(), 'f1db')
const files = await fs.promises.readdir(DB_PATH);

const mongoClient = new MongoClient(process.env.MONGO_DB_URL, {
	serverApi: ServerApiVersion.v1,
});

await mongoClient.connect();

const db = mongoClient.db('f1db');

for (const file of files) {
	const content = await fs.promises.readFile(path.resolve(DB_PATH, file), 'utf8');
	const collection_name = file.replace(path.extname(file), '');
	const collection = db.collection(collection_name);
	const { data, errors } = Papa.parse(content.trim(), {
		header: true,
		fastMode: false,
		dynamicTyping: true,
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

	let start = 0;

	while (start < data.length) {
		const batchData = data.slice(start, start + BATCH_SIZE);

		console.log(
			`Collection: ${collection_name}: Inserting ${start} - ${start + batchData.length} of ${data.length}`,
		);
		await collection.insertMany(batchData);

		start += BATCH_SIZE;
	}
}

await mongoClient.close(true);
