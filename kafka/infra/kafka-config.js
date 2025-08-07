import {
    Writer,
    Reader,
    Connection,
    SchemaRegistry,
    VALUE,
    TOPIC_NAME_STRATEGY,
    SCHEMA_TYPE_AVRO,
    CODEC_SNAPPY
} from "k6/x/kafka"; // import kafka extension
import {
    brokers,
    topic,
    registry,
} from "../utils/envs.js";

const valueSchema = open('../schemas/order-value.avsc');

const connection = new Connection({
    address: brokers[0],
});

const schemaRegistry = new SchemaRegistry({
    url: registry
});

const writer = new Writer({
    brokers: brokers,
    topic: topic,
    autoCreateTopic: true,
    compression: CODEC_SNAPPY
});

const reader = new Reader({
    brokers: brokers,
    groupId: `k6-kafka-order-group-${__VU}`,
    groupTopics: [topic],
});

const valueSubjectName = schemaRegistry.getSubjectName({
    topic: topic,
    element: VALUE,
    subjectNameStrategy: TOPIC_NAME_STRATEGY,
    schema: valueSchema,
});

const valueSchemaObject = schemaRegistry.createSchema({
    subject: valueSubjectName,
    schema: valueSchema,
    schemaType: SCHEMA_TYPE_AVRO,
});

function createTopicIfNotExists() {
    connection.createTopic({
        topic: topic,
        numPartitions: 6,
        replicationFactor: 1,
        configEntries: [
            {
                configName: "compression.type",
                configValue: CODEC_SNAPPY,
            },
        ],
    })
}

function closeAll() {
    writer.close();
    reader.close();
    connection.close();
    console.log("All connections closed");
}

function deleteTopicMessages() {
    connection.deleteTopic(topic);
    console.log(`Topic ${topic} deleted`);
}

export {
    connection,
    schemaRegistry,
    writer,
    reader,
    valueSchemaObject,
    topic,
    brokers,
    valueSubjectName,
    createTopicIfNotExists,
    closeAll,
    deleteTopicMessages
}