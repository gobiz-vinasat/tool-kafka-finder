const {Kafka} = require('kafkajs');
const MongoClient = require('mongodb').MongoClient;
const {exec} = require('child_process');
const os = require('os');
const config = require('./config');

(async () => {
    const mongo = await connectMongo();
    const db = mongo.db(config.mongo.db);
    const consumer = await createConsumer();

    async function connectMongo() {
        const mongo = new MongoClient(`mongodb://${config.mongo.username}:${config.mongo.password}@${config.mongo.host}:${config.mongo.port}`, {useUnifiedTopology: true});
        await mongo.connect();

        return mongo;
    }

    async function createConsumer() {
        const kafka = new Kafka({
            clientId: config.kafka.clientId,
            brokers: config.kafka.brokers,
            logCreator,
        });

        const consumer = kafka.consumer({
            groupId: config.kafka.groupId,
        });

        await consumer.connect();

        for (const topic of config.kafka.topics) {
            await consumer.subscribe({topic});
        }

        return consumer;
    }

    function logCreator(logLevel) {
        return async ({namespace, level, label, log}) => {
            console.log({namespace, level, label, log});

            if (log.message.includes('Connection error: read ECONNRESET')) {
                return restartContainer();
            }
        }
    }

    async function restartContainer() {
        const command = `curl --unix-socket /var/run/docker.sock -X POST http:/v1.24/containers/${os.hostname()}/restart`;
        return new Promise((resolve, reject) => exec(command, (error, stdout) => error ? reject(error) : resolve(stdout)));
    }

    async function eachMessage({topic, partition, message}) {
        await saveEvent({
            topic,
            partition: Number(partition),
            offset: Number(message.offset),
            key: message.key.toString(),
            timestamp: new Date(Number(message.timestamp)),
        });
    }

    async function saveEvent(event) {
        await db.collection('events').updateOne({
                topic: event.topic,
                key: event.key
            }, {
                $set: event,
                $setOnInsert: {firstOffset: event.offset},
            }, {upsert: true}
        );

        if (config.debug) {
            console.log({event});
        }
    }

    function handleProcessError() {
        const errorTypes = ['unhandledRejection', 'uncaughtException'];
        const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

        errorTypes.map(type => {
            process.on(type, async e => {
                try {
                    console.log(`process.on ${type}`);
                    console.error(e);
                    await terminate();
                    process.exit(0)
                } catch (_) {
                    process.exit(1)
                }
            })
        });

        signalTraps.map(type => {
            process.once(type, async () => {
                try {
                    await terminate();
                } finally {
                    process.kill(process.pid, type);
                }
            });
        });
    }

    async function terminate() {
        await consumer.disconnect();
        await mongo.close();
    }

    consumer.run({eachMessage});

    handleProcessError();
})();
