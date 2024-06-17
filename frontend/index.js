const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const { Kafka } = require('kafkajs');


const app = express();
const server = http.createServer(app);
const io = socketIo(server);

const kafka = new Kafka({
    clientId: 'rpg-frontend',
    brokers: ['localhost:19092'] // Your Kafka broker
  });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'rpg-group' });
const bonusConsumer = kafka.consumer({ groupId: 'bonus-group' });

const setupKafka = async () => {
    await producer.connect();
    await consumer.connect();
    await bonusConsumer.connect();
    await consumer.subscribe({ topic: 'rpg-response' });
    await bonusConsumer.subscribe({ topic: 'bonus' });

    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const value = message.value.toString();  // Convert buffer to string
          const jsonData = JSON.parse(value);  // Parse JSON string to an object
          io.emit('receive-message', jsonData);  // Emit as a JavaScript object
        },
      });

    bonusConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const value = message.value.toString();  
          const jsonData = JSON.parse(value);  
          io.emit('bonus-message', jsonData);  
        },
    });
  };

  setupKafka();

  io.on('connection', (socket) => {
    socket.on('send-message', async (data) => {
      //io.emit('receive-message', data); 
      await producer.send({
        topic: 'npc-request',
        messages: [
          { value: JSON.stringify(data) }
        ],
      });
    });
});

app.use(express.static('public'));

server.listen(8080, () => {
  console.log('Server running on http://localhost:3000');
});
