/**
 * @link https://www.github.com/blacksmoke26/amqp-node-rpc-direct.git
 * @copyright Copyright (c) 2024 Junaid Atari
 * @license https://www.apache.org/licenses/LICENSE-2.0.txt
 * @author Junaid Atari <mj.atari@gmail.com>
 */

import RabbitMQClient from '../core/RabbitMQClient.js';

// utils
import { QUEUE_RPC } from '../utils/queues.js';

const client = new RabbitMQClient();

async function main () {
  if (!await client.connect()) return; // Shutdown if connection fails

  console.log(`[ ${new Date()} ] Producer started`);

  const response = await client.sendRpc(QUEUE_RPC, JSON.stringify('John Doe'));

  console.log('response:', response);
}

main();