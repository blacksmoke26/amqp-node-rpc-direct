/**
 * @link https://www.github.com/blacksmoke26/amqp-node-rpc-direct.git
 * @copyright Copyright (c) 2024 Junaid Atari
 * @license https://www.apache.org/licenses/LICENSE-2.0.txt
 * @author Junaid Atari <mj.atari@gmail.com>
 */

import { randomUUID } from 'node:crypto';

import amqp from 'amqplib';
import merge from 'deepmerge';

/**
 * RabbitMQ client.
 * @see https://www.rabbitmq.com/
 */
export default class RabbitMQClient {
  /**
   * Connection object
   * @type {import('amqplib').Connection}
   */
  #connection;

  /**
   * Connect to RabbitMQ server.
   * @param {string|import('amqplib').Options.Connect} [url] - Connection URL (e.g., amqp://localhost)
   * @returns {Promise<import('amqplib').Connection|null>} - The connection object / null if failed.
   * @see https://amqp-node.github.io/amqplib/channel_api.html#connect
   */
  async connect (url = null) {
    try {
      this.#connection = await amqp.connect(url || 'amqp://localhost');
      return this.getConnection();
    } catch (e) {
      console.error('Error connecting to RabbitMQ:', e);
      return null;
    }
  }

  /**
   * Get the connection object.
   * @returns {import('amqplib').Connection} - The connection object.
   * @throws {Error} - If the connection is not established.
   */
  getConnection () {
    if (!this.#connection) {
      throw new Error('RabbitMQ connection not established');
    }
    return this.#connection;
  }

  /**
   * Close the connection.
   * @returns {Promise<void>}
   */
  async close () {
    await this.getConnection().close();
  }

  /**
   * Create a channel.
   * @returns {Promise<import('amqplib').Channel>}
   */
  async createChannel () {
    return this.getConnection().createChannel();
  }

  /**
   * Send a message to a queue.
   * @param {string} queue - The queue name.
   * @param {string|Record<string, any>} message - The message to send. The message will be converted to JSON.
   * @param {Record<string, any>} [options] - The options for the queue.
   * @param {import('amqplib').Options.AssertQueue} [options.assert] - The options for asserting the queue.
   * @param {import('amqplib').Options.Publish} [options.send] - The options for the send queue.
   * @param {import('amqplib').Channel} [options.closeChannel] - Whether to close the channel after sending the message.
   * @param {boolean} [options.channel] - Channel to use. If not specified, the default channel will be used.
   * @returns {Promise<boolean>} - Whether the message was sent.
   * @see Options `assert` for [assertQueue](https://amqp-node.github.io/amqplib/channel_api.html#channel_assertQueue)
   * @see Options `send` for [sendToQueue](https://amqp-node.github.io/amqplib/channel_api.html#channel_sendToQueue)
   */
  async sendQueue (queue, message, options = {}) {
    /** @type {import('amqplib').Options.AssertQueue} */
    const assertOptions = merge({
      durable: true,
      autoDelete: false
    }, options?.assert ?? {});

    /** @type {import('amqplib').Options.Publish} */
    const sendOptions = merge({
      persistent: true
    }, options?.send ?? {});

    const channel = options?.channel || await this.createChannel();
    await channel.assertQueue(queue, assertOptions);

    const response = channel.sendToQueue(
      queue, RabbitMQClient.messageEncode(message), sendOptions
    );

    if (options?.closeChannel) {
      await channel.close();
    }

    return response;
  }

  /**
   * Delete a queue.
   * @param {string} queue - The queue name.
   * @param {Record<string, any>} [options] - The options for the queue.
   * @param {boolean} [options.channel] - Channel to use. If not specified, the default channel will be used.
   * @returns {Promise<import('amqplib').Options.DeleteQueue>}
   */
  async deleteQueue (queue, options = {}) {
    const channel = options?.channel || await this.createChannel();
    return channel.deleteQueue(queue);
  }

  /**
   * Encode a message
   * @param {Record<string, any>|any} obj - The message to encode.
   * @returns {Buffer} - The Buffer of the encoded message.
   * @public
   * @static
   */
  static messageEncode (obj) {
    return Buffer.from(JSON.stringify(obj));
  }

  /**
   * Decode a message
   * @param {import('amqplib').ConsumeMessage} message - The message to decode.
   * @returns {Record<string, any>|any|null} - The decoded message or null if failed.
   * @public
   * @static
   */
  static messageDecode (message) {
    return !message ? null : JSON.parse(message?.content?.toString?.() || {});
  }

  /**
   * Send an RPC request.
   * @template T - The type of the response.
   * @param {string} queue - The queue name.
   * @param {string|number|Record<string, any>} message - The message to send.
   * @returns {Promise<T>} - The response.
   */
  async sendRpc (queue, message) {
    const correlationId = randomUUID();

    const channel = await this.getConnection().createChannel();
    channel.setMaxListeners(0);

    const q = await channel.assertQueue(queue, { durable: false });
    await channel.consume(q.queue, async msg => {
      if (msg && msg.properties.correlationId === correlationId) {
        channel.emit(correlationId, msg.content.toString('utf8'));

        channel.ack(msg); // Acknowledge the message
        await channel.cancel(correlationId); // Cancel the correlationId
      }
    }, {
      noAck: false
    });

    return new Promise(resolve => {
      channel.once(correlationId, resolve);
      channel.sendToQueue(queue, Buffer.from(message), {
        correlationId,
        replyTo: q.queue,
      });
    });
  }
}
