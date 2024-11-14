/**
 * @link https://www.github.com/blacksmoke26/amqp-node-rpc-direct.git
 * @copyright Copyright (c) 2024 Junaid Atari
 * @license https://www.apache.org/licenses/LICENSE-2.0.txt
 * @author Junaid Atari <mj.atari@gmail.com>
 */

import RabbitMQClient from '../core/RabbitMQClient.js';
import { QUEUE_RPC } from '../utils/queues.js';

async function main () {
  const client = new RabbitMQClient();
  if (!await client.connect()) return; // Shutdown if connection fails

  console.log(`[ ${new Date()} ] Consumer service started`);

  const channel = await client.createChannel();

  await channel.assertQueue(QUEUE_RPC, {
    durable: false
  });

  await channel.prefetch(1); // Prefetch 1 message
  await channel.consume(QUEUE_RPC, message => {
    if (!message) return; // No message, just return

    const data = JSON.parse(message.content.toString('utf8'));

    console.log(
      `[ ${new Date()} ] Message received: ${JSON.stringify(
        JSON.parse(message.content.toString('utf8')),
      )}`,
    );

    const response = {
      data: data.toUpperCase(),
    };

    console.log(
      `[ ${new Date()} ] Message sent: ${JSON.stringify(response)}`,
    );

    channel.sendToQueue(
      message.properties.replyTo, Buffer.from(JSON.stringify(response)), {
        correlationId: message.properties.correlationId,
      }
    );

    channel.ack(message); // Acknowledge the message
  });
}

main();