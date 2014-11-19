/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pivotal

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import scala.reflect.ClassTag

/**
 * Utility functions for creating RabbitMQDStream
 */
object RabbitMQUtil {
  def defaultStringDeserializer(bytes : Array[Byte]) : String = {
    new String(bytes)
  }

  /**
   * create RabbitMQ DStreaming
   * @param ssc spark streaming context
   * @param deserializer deserialization method to parse byte array into object
   * @param host RabbitMQ broker hostname
   * @param port RabbitMQ broker service port
   * @param queue The name of the subscribed message queue
   * @param exchange The name of the exchange to bind, default is null
   * @param user The user to connect to the broker, default is null
   * @param password The password to connect to the broker, default is null
   * @param prefetch The prefetch count for streaming consumer, deault is 0
   * @param storageLevel Storage level for stream rdd, default value is MEMORY_AND_DISK_2
   * @return RabbitMQ Input DStream
   */
  def createStream[T :ClassTag](ssc :StreamingContext,
      deserializer :Array[Byte] => T,
      host :String, port :Int, queue :String, exchange :String = null,
      user :String = null, password :String = null, prefetch :Int = 0,
      storageLevel :StorageLevel = StorageLevel.MEMORY_AND_DISK_2)
      : ReceiverInputDStream[T] = {
    var props = Map( RabbitMQReceiver.BrokerHostKey -> host,
        RabbitMQReceiver.BrokerPortKey -> port.toString(),
        RabbitMQReceiver.QueueNameKey -> queue
    )
    if (exchange != null) {
      props += (RabbitMQReceiver.ExchangeNameKey -> exchange)
    }
    if (user != null) {
      props += (RabbitMQReceiver.UserKey -> user)
    }
    if (password != null) {
      props += (RabbitMQReceiver.PasswdKey -> password)
    }
    if (prefetch > 0)  {
      props += (RabbitMQReceiver.PrefetchCountKey -> prefetch.toString)
    }
    new RabbitMQInputDStream(ssc, props, deserializer, storageLevel)
  }

  /**
   * create RabbitMQ String DStreaming
   * @param ssc spark streaming context
   * @param host RabbitMQ broker hostname
   * @param port RabbitMQ broker service port
   * @param queue The name of the queue to subscribe
   * @param exchange The name of the exchange to bind, default is null
   * @param user The user to connect to the broker, default is null
   * @param password The password to connect to the broker, default is null
   * @param prefetch The prefetch count for streaming consumer, deault is 0
   * @param storageLevel Storage level for stream rdd, default value is MEMORY_AND_DISK_2
   * @return RabbitMQ Input DStream
   */
  def createStrStream(ssc :StreamingContext,
      host :String, port :Int, queue :String, exchange :String = null,
      user :String = null, password :String = null, prefetch :Int = 0,
      storageLevel :StorageLevel = StorageLevel.MEMORY_AND_DISK_2)
      : ReceiverInputDStream[String] = {
    createStream(ssc, defaultStringDeserializer, host, port, queue, exchange,
      user, password, prefetch, storageLevel)
  }

  /**
   * create RabbitMQ DStream
   * @param ssc spark streaming context
   * @param deserializer deserialization method to parse byte array into object
   * @param uri uri for RabbitMQ broker
   * @param queue the name of queue to subscribe
   * @param exchange the name of exchange to bind, default is null
   * @param prefetch the prefetch count for stream consumer, default is 0
   * @param storageLevel storage level for rdd
   * @return RabbitMQ Input DStream
   */
  def createStreamWithUri[T :ClassTag](ssc :StreamingContext,
      deserializer :Array[Byte] => T,
      uri :String, queue :String, exchange :String = null, prefetch :Int = 0,
      storageLevel :StorageLevel = StorageLevel.MEMORY_AND_DISK_2)
      :ReceiverInputDStream[T] = {
    var props = Map (
      RabbitMQReceiver.BrokerUriKey -> uri
    )
    if (queue != null) {
      props += (RabbitMQReceiver.QueueNameKey -> queue)
    }
    if (exchange != null) {
      props += (RabbitMQReceiver.ExchangeNameKey -> exchange)
    }
    if (prefetch > 0)  {
      props += (RabbitMQReceiver.PrefetchCountKey -> prefetch.toString())
    }
    new RabbitMQInputDStream(ssc, props, deserializer, storageLevel)
  }

  /**
   * create RabbitMQ String DStream
   * @param ssc spark streaming context
   * @param uri uri for RabbitMQ broker
   * @param queue the name of queue to subscribe
   * @param exchange the name of exchange to bind, default is null
   * @param prefetch the prefetch count for stream consumer, default is 0
   * @param storageLevel storage level for rdd
   * @return RabbitMQ Input DStream
   */
  def createStrStreamWithUri(ssc :StreamingContext, uri :String, queue :String,
      exchange :String = null, prefetch :Int = 0,
      storageLevel :StorageLevel = StorageLevel.MEMORY_AND_DISK_2)
      : ReceiverInputDStream[String] = {
    createStreamWithUri(ssc, defaultStringDeserializer, uri, queue, exchange,
        prefetch, storageLevel)
  }

  /**
   * create RabbitMQ DStreaming
   * @param ssc spark streaming context
   * @param deserializer deserialization method to parse byte array into object
   * @param conf a string to string map which contains all the conf
   *             parameters for consumer
   * @return RabbitMQ Input Stream
   */
  def createStreamWithConf[T :ClassTag](ssc :StreamingContext,
      deserializer :Array[Byte] => T,
      conf :Map[String, String],
      storageLevel :StorageLevel = StorageLevel.MEMORY_AND_DISK_2)
      :ReceiverInputDStream[T] = {
    new RabbitMQInputDStream(ssc, conf, deserializer, storageLevel)
  }

  /**
   * create RabbitMQ DStreaming
   * @param ssc spark streaming context
   * @param conf a string to string map which contains all the conf
   *             parameters for consumer
   * @return RabbitMQ Input Stream
   */
  def createStrStreamWithConf(ssc :StreamingContext,
      conf :Map[String, String],
      storageLevel :StorageLevel = StorageLevel.MEMORY_AND_DISK_2)
      :ReceiverInputDStream[String] = {
    createStreamWithConf(ssc, defaultStringDeserializer, conf, storageLevel)
  }
}
