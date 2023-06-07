import { Collection, MongoClient } from "mongodb";
import { ICustomer } from "./app";

const url = process.env.DB_URI;

if (!url) {
  console.error(`please define DB_URI in .env or in environment variables`);
  process.exit(1);
}

const mongoClient = new MongoClient(url);
const dbName = "test-project";

type CollectionNames = "customers" | "customers_anonymised";

let isConnected = false;

export async function getCollection(
  collectionName: CollectionNames
): Promise<Collection<ICustomer>> {
  if (!isConnected) {
    await mongoClient.connect();
    isConnected = true;
  }

  return mongoClient.db(dbName).collection<ICustomer>(collectionName);
}
