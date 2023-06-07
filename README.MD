How to run program?

* Install nodejs
* prepare mongodb with replica-set configuration
* rename .env.local file into .env and replace DB_URI="mongodb://localhost:27017/" string with your real MongoDB connection string
* run npm i

For sync all data run
```
npm run sync-full-reindex-dev
```

For real time sync run
```
npm run sync-dev
```

For filling customer collection with fake data run 
```
npm run app-dev
```
