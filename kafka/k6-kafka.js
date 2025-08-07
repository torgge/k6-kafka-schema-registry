/*
This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 1 Avro message per iteration.
*/

import { check, sleep } from "k6";
import { b64encode, b64decode } from "k6/encoding";
import {
  SCHEMA_TYPE_AVRO
} from "k6/x/kafka"; // import kafka extension
import { Trend } from "k6/metrics"; // import k6 metrics
import * as utils from "./utils/utils.js";
import { quantityOfMessages } from "./utils/envs.js";
import {
  schemaRegistry,
  writer,
  reader,
  valueSchemaObject,
  createTopicIfNotExists,
  closeAll,
  deleteTopicMessages
} from "./infra/kafka-config.js";

export const options = {
  scenarios: {
    default: {
      executor: 'per-vu-iterations',
      vus: 1,
      iterations: 1,
    },
  },
};

function getItems(index) {
  let items = [];
  let rows = Math.floor(Math.random() * 50) + 1; // Random number of items between 1 and 50
  for (let i = 0; i < rows; i++) {
    items.push({
      sku: `${index}-${i}`,
      productName: `product-${index}-${i}`,
      quantity: Math.floor(Math.random() * 10) + 1
    });
  }
  return items;
}

export let produceDuration = new Trend("produce_duration", true);

export function setup() {
  if (__VU == 0) {
    createTopicIfNotExists();
  }
}

export default function () {
  const start = new Date().getTime();

  for (let index = 0; index < quantityOfMessages; index++) {
    let correlationId = `${utils.uuidv4()}`;
    let messages = [
      {
        key: b64encode('ROUTER'), // Base64 encode using k6's function
        value: schemaRegistry.serialize({
          data: {
            id: index,
            content: JSON.stringify({
              clientName: `client-${index}`,
              items: getItems(index),
            }),
          },
          schema: valueSchemaObject,
          schemaType: SCHEMA_TYPE_AVRO,
        }),
        headers: {
          "correlationId": `${correlationId}`,
          "origin": "k6-kafka-test",
          "timestamp": new Date().toISOString()
        }
      },
    ];
    writer.produce({ messages: messages });
    sleep(1); // Sleep to simulate some delay between message production
  }
  // Wait for the messages to be produced
  produceDuration.add(new Date().getTime() - start);

  let messages = reader.consume({ limit: quantityOfMessages });

  messages.forEach(msg => {
    const deserializedValue = schemaRegistry.deserialize({
      data: msg.value,
      schema: valueSchemaObject,
      schemaType: SCHEMA_TYPE_AVRO,
    });
    const contentData = JSON.parse(deserializedValue.content);
    console.log(`Consumed message - ID: ${deserializedValue.id}, Content: ${JSON.stringify(contentData.items[0])}`);
  });

  check(messages, {
    [`${quantityOfMessages} message returned`]: (msgs) => msgs.length == quantityOfMessages,
    "key is 'ROUTER'": (msgs) => {
      // Decode base64 key using k6's function
      const key = b64decode(msgs[0].key, 'std', 's');
      return key === 'ROUTER';
    },
    "value contains 'clientName-' and 'productName-' strings": (msgs) => {
      const deserializedValue = schemaRegistry.deserialize({
        data: msgs[0].value,
        schema: valueSchemaObject,
        schemaType: SCHEMA_TYPE_AVRO,
      });
      const contentData = JSON.parse(deserializedValue.content);
      return contentData.clientName.startsWith("client-") &&
             contentData.items[0].productName.startsWith("product-");
    },
  });
}

export function teardown() {
  if (__VU == 0) {
    // Delete the topic
    deleteTopicMessages();
  }
  closeAll();
}