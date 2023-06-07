import { faker, ne } from "@faker-js/faker";
import { ICustomer } from "./app";
import { getCollection } from "./db";
import { Filter, ObjectId, WithId } from "mongodb";

function anonymizeCustomer(customer: WithId<ICustomer>): WithId<ICustomer> {
  return {
    ...customer,
    firstName: faker.string.alphanumeric(8),
    lastName: faker.string.alphanumeric(8),
    address: {
      ...customer.address,
      line1: faker.string.alphanumeric(8),
      line2: faker.string.alphanumeric(8),
      postcode: faker.string.alphanumeric(8),
    },
    email: [faker.string.alphanumeric(8), customer.email.split("@").pop()].join(
      "@"
    ),
  };
}

const buffer = new Set<WithId<ICustomer>>();

async function run() {
  if (process.argv.includes("--full-reindex")) {
    return syncAllData();
  }

  const lastSavedId = await syncHistoricalData();

  const customerCollection = await getCollection("customers");

  const cursor = customerCollection.watch<ICustomer>(
    [{ $match: { operationType: { $in: ["insert", "update", "replace"] } } }],
    {
      fullDocument: "updateLookup",
    }
  );

  let isWatcherStarted = false;

  cursor.on("change", async (change) => {
    // Watcher starts after some delay. We should run one time re-sync data from last stop after watcher start working
    // Without it, if some new data was ingested into DB after syncHistoricalData and before watcher started
    // we missed some documents
    if (!isWatcherStarted) {
      await syncAfterId(lastSavedId);
      isWatcherStarted = true;
    }

    if (
      change.operationType === "replace" ||
      change.operationType === "insert" ||
      change.operationType === "update"
    ) {
      await addCustomerIntoBuffer(
        anonymizeCustomer(change.fullDocument as WithId<ICustomer>)
      );
    }
  });

  setInterval(async () => {
    await saveBuffer();
  }, 1000);
}

async function syncAllData() {
  await syncAfterId();

  await saveBuffer();

  process.exit(0);
}

async function syncHistoricalData(): Promise<ObjectId> {
  const anonymizedCustomerCollection = await getCollection(
    "customers_anonymised"
  );

  const lastAnonymized = await anonymizedCustomerCollection.findOne(
    {},
    { sort: { _id: -1 }, projection: { _id: 1 } }
  );

  let lastSavedId = await syncAfterId(lastAnonymized?._id);
  let previousSavedId: ObjectId | undefined = lastSavedId;

  while (lastSavedId) {
    lastSavedId = await syncAfterId(lastSavedId);
    previousSavedId = lastSavedId || previousSavedId;
  }

  return previousSavedId;
}

async function syncAfterId(id?: string | ObjectId): Promise<ObjectId> {
  const customerCollection = await getCollection("customers");

  const filter: Filter<ICustomer> = {};

  if (Boolean(id)) {
    filter._id = { $gt: new ObjectId(id) };
  }
  const cursor = customerCollection.find(filter, { sort: { _id: 1 } });

  let lastSavedId: ObjectId | undefined;

  for await (const customer of cursor) {
    await addCustomerIntoBuffer(anonymizeCustomer(customer));
    lastSavedId = customer._id;
  }

  console.log(
    `All previous customers was proceeded ${new Date().toISOString()}`,
    {
      startedId: id,
      finishedId: lastSavedId,
    }
  );

  return lastSavedId;
}

async function addCustomerIntoBuffer(
  customer: WithId<ICustomer>
): Promise<void> {
  buffer.add(customer);

  if (buffer.size >= 1000) {
    await saveBuffer();
  }
}

async function saveBuffer(): Promise<void> {
  if (buffer.size === 0) {
    console.log(`${Date.now()} There are no customers in buffer for saving.`);
    return;
  }

  const bufferCopy = [...buffer];
  buffer.clear();

  const collection = await getCollection("customers_anonymised");

  const result = await collection.bulkWrite(
    bufferCopy.map(({ _id, ...rest }) => ({
      replaceOne: { filter: { _id }, upsert: true, replacement: rest },
    }))
  );

  console.log({ time: Date.now(), result });
}

run().catch((error) => {
  console.error(`something went wrong`, { error });
});
