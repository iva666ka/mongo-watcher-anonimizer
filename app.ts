import { faker } from "@faker-js/faker";
import { getCollection } from "./db";

export interface ICustomer {
  firstName: string;
  lastName: string;
  email: string;
  address: {
    line1: string;
    line2: string;
    postcode: string;
    city: string;
    state: string;
    country: string;
  };
  createdAt: Date;
}

function createRandomUser(): ICustomer {
  return {
    firstName: faker.person.firstName(),
    lastName: faker.person.lastName(),
    email: faker.internet.email(),
    address: {
      line1: faker.location.streetAddress(),
      line2: faker.location.secondaryAddress(),
      postcode: faker.location.zipCode(),
      city: faker.location.city(),
      state: faker.location.state(),
      country: faker.location.country(),
    },
    createdAt: faker.date.past(),
  };
}

function createMultipleUsers() {
  return faker.helpers.multiple(createRandomUser, {
    count: faker.number.int({ min: 1, max: 10 }),
  });
}

async function run() {
  const collection = await getCollection("customers");

  setInterval(async () => {
    const newCustomers = createMultipleUsers();

    const result = await collection.insertMany(newCustomers);
    console.log({ result });
  }, 200);
}

run().catch((err) => {
  console.error(`something went wrong`, { err });
});
