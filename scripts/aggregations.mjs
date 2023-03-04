import 'dotenv/config';
import { MongoClient, ServerApiVersion } from 'mongodb';
import { Collections } from './utils.mjs';

const mongoClient = new MongoClient(process.env.MONGO_DB_URL, {
	serverApi: ServerApiVersion.v1,
});

await mongoClient.connect();

const db = mongoClient.db(process.env.MONGO_DB_NAME);

console.log('Driver standings final');
await db
	.collection(Collections.Races)
	.aggregate([
		{ $group: { _id: '$year', round: { $max: '$round' } } },
		{ $project: { _id: 0, year: '$_id', round: 1 } },
		{
			$lookup: {
				from: Collections.Races,
				as: 'race',
				localField: 'round',
				foreignField: 'round',
				let: { year: '$year' },
				pipeline: [{ $match: { $expr: { $eq: ['$year', '$$year'] } } }],
			},
		},
		{ $unwind: { path: '$race' } },
		{ $project: { year: 1, round: 1, raceId: '$race.raceId' } },
		{
			$lookup: {
				from: Collections.DriverStandings,
				as: 'driver',
				localField: 'raceId',
				foreignField: 'raceId',
			},
		},
		{ $unwind: { path: '$driver' } },
		{ $replaceRoot: { newRoot: { $mergeObjects: [{ year: '$year' }, '$driver'] } } },
		{ $out: Collections.DriverStandingsFinal },
	])
	.toArray();

console.log('Constructor standings final');
await db
	.collection(Collections.Races)
	.aggregate([
		{ $group: { _id: '$year', round: { $max: '$round' } } },
		{ $project: { _id: 0, year: '$_id', round: 1 } },
		{
			$lookup: {
				from: Collections.Races,
				as: 'race',
				localField: 'round',
				foreignField: 'round',
				let: { year: '$year' },
				pipeline: [{ $match: { $expr: { $eq: ['$year', '$$year'] } } }],
			},
		},
		{ $unwind: { path: '$race' } },
		{ $project: { year: 1, round: 1, raceId: '$race.raceId' } },
		{
			$lookup: {
				from: Collections.ConstructorStandings,
				as: 'constructor',
				localField: 'raceId',
				foreignField: 'raceId',
			},
		},
		{ $unwind: { path: '$constructor' } },
		{ $replaceRoot: { newRoot: { $mergeObjects: [{ year: '$year' }, '$constructor'] } } },
		{ $out: Collections.ConstructorStandingsFinal },
	])
	.toArray();

await mongoClient.close(true);

process.exit(0);
