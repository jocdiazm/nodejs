const express = require("express");
const { KafkaConsumer, Message } = require("node-rdkafka");
const dotenv = require("dotenv");
const cors = require("cors");
console.log("start ");
dotenv.config();

const app = express();

//enable CORS
app.use(cors());

const port = 8000;

// Set up Kafka consumer
const consumer = KafkaConsumer({
  "bootstrap.servers": process.env.BOOTSTRAP_SERVERS,
  "sasl.username": process.env.SASL_USERNAME,
  "sasl.password": process.env.SASL_PASSWORD,
  "security.protocol": process.env.SECURITY_PROTOCOL,
  "sasl.mechanisms": "PLAIN",
  "group.id": "conforca",
  "auto.offset.reset": "latest", // Start consuming from the beginning of the topic
});

const receivedMessages = [];
let isConsuming = false; // Track the consumer state

// Route to handle accident events
app.post("/api/accidents", async (req, res) => {
  const accidentEvent = req.body;

  try {
    // Produce the accident event to Kafka topic
    producer.produce(
      "accidents",
      null,
      Buffer.from(JSON.stringify(accidentEvent)),
      null,
      Date.now()
    );

    res.sendStatus(200);
  } catch (error) {
    console.error("Error producing accident event:", error);
    res.sendStatus(500);
  }
});

const emitSSE = (res, id, data) => {
  res.write("id: " + id + "\n");
  res.write("data: " + data + "\n\n");
};

const handleSSE = (req, res) => {
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
  });
  const id = new Date().toLocaleTimeString();
  // Sends a SSE every 3 seconds on a single connection.
  setInterval(function () {
    emitSSE(res, id, new Date().toLocaleTimeString());
  }, 3000);

  emitSSE(res, id, new Date().toLocaleTimeString());
};

//use it

app.get("/stream", handleSSE);

// Route to get the list of received accident messages
app.get("/api/accidents_mocked", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");

  // Send initial response with received messages
  res.write(`data: ${JSON.stringify(receivedMessages)}\n\n`);
  // const now = new Date();
  // console.log(`🚀 receivedMessages at ${now.toLocaleTimeString()})}`);

  //Keep the connection alive
  const heartbeatInterval = setInterval(() => {
    res.write("ping\n\n");
  }, 10000);

  // Function to send new messages as server-side events
  const sendServerSentEvent = (message) => {
    res.write(`data: ${JSON.stringify(message)}\n\n`);
  };

  // Event handler when new message is consumed
  const messageHandler = (message) => {
    const accident = JSON.parse(message.value.toString());
    receivedMessages.push(accident);
    sendServerSentEvent(accident);
  };

  // Start consuming messages if not already consuming
  if (!isConsuming) {
    isConsuming = true;
    consumer.connect();
    consumer.on("ready", () => {
      // console.log("Consumer is ready playertest");
      consumer.subscribe(["accidents_cdc"]);
      consumer.consume();
    });
  }

  // Event handler for received messages
  consumer.on("data", messageHandler);

  // Event handler for errors
  consumer.on("event.error", (error) => {
    console.error("Kafka consumer error:", error);
  });

  // Event handler when client closes the connection
  req.on("close", () => {
    clearInterval(heartbeatInterval);
    consumer.unsubscribe();
    consumer.disconnect();
  });
});

// Start consuming messages if not already consuming
consumer.connect();
consumer.on("ready", () => {
  console.log("Consumer is ready playerone");
  consumer.subscribe(["accidents_cdc"]);
  consumer.consume();
});

app.get("/api/accidents", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");

  // Send initial response with received messages
  res.write(`data: ${JSON.stringify(receivedMessages)}\n\n`);

  //Keep the connection alive
  const heartbeatInterval = setInterval(() => {
    res.write(": heartbeat\n\n");
  }, 10000);

  // Function to send new messages as server-side events
  const sendServerSentEvent = (message) => {
    res.write(`data: ${JSON.stringify(message)}\n\n`);
    console.log(message);
  };

  // Event handler when new message is consumed
  const messageHandler = (message) => {
    const accident = JSON.parse(message.value.toString());
    receivedMessages.push(accident);
    sendServerSentEvent(accident);
  };

  // Event handler for received messages
  consumer.on("data", messageHandler);

  // Event handler for errors
  consumer.on("event.error", (error) => {
    console.error("Kafka consumer error:", error);
  });

  // Event handler when client closes the connection
  req.on("close", () => {
    clearInterval(heartbeatInterval);
  });
});

// // Start the Kafka consumer
// consumer.connect();

// consumer.on("ready", () => {
//   console.log("Consumer is ready");

//   // Subscribe to the 'accidents' topic
//   consumer.subscribe(["accidents"]);

//   // Consume messages from the topic
//   consumer.consume();
// });

// consumer.on("data", (message) => {
//   const accident = JSON.parse(message.value.toString());
//   receivedMessages.push(accident);
// });

consumer.on("event.error", (error) => {
  console.error("Kafka consumer error:", error);
});

// consumer.unsubscribe();
// consumer.disconnect();

// Start the server
app.listen(port, () => {
  console.log(`Server is listening on port ${port}`);
});
