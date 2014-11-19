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

/**
 * RabbitMQ receiver for Spark DStream
 */
package io.pivotal

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
import com.rabbitmq.client._

import scala.reflect.ClassTag

class RabbitMQReceiver[T :ClassTag](val props: Map[String, String],
    deserializer : Array[Byte] => T,
    storageLevel : StorageLevel = StorageLevel.MEMORY_AND_DISK_2) extends
    Receiver[T](storageLevel) with Logging {
  var factory : ConnectionFactory = null

  if (props.get(RabbitMQReceiver.QueueNameKey).getOrElse("").isEmpty &&
    props.get(RabbitMQReceiver.ExchangeNameKey).getOrElse("").isEmpty) {
    throw new IllegalArgumentException(s"Both ${RabbitMQReceiver.QueueNameKey} and" +
      s"${RabbitMQReceiver.ExchangeNameKey} are not set.")
  }

  def onStart() {
    factory = new ConnectionFactory()
    var brokerUriIsSet = false
    props.get(RabbitMQReceiver.BrokerUriKey).foreach(v => {
      logInfo(s"set rabbit connection factory with uri:${v}")
      factory.setUri(v)
      brokerUriIsSet = true
    })
    if (!brokerUriIsSet)  {
      props.foreach( entry => {
        entry._1 match {
          case RabbitMQReceiver.BrokerHostKey => factory.setHost(entry._2)
          case RabbitMQReceiver.BrokerPortKey => factory.setPort(entry._2.toInt)
          case RabbitMQReceiver.UserKey => factory.setUsername(entry._2)
          case RabbitMQReceiver.PasswdKey => factory.setPassword(entry._2)
          case RabbitMQReceiver.VirtualHostKey => factory.setVirtualHost(
            entry._2)
          case RabbitMQReceiver.PrefetchCountKey | RabbitMQReceiver
            .QueueNameKey | RabbitMQReceiver.ExchangeNameKey => {} //do nothing
          case _ => logWarning("unknown property for RabbitMQReceiver. " +
            s"key:${entry._1} value:${entry._2}")
        }
      })
    }
    // Start the thread that receives data from a rabbitMQ broker
    new Thread("Rabbit MQ Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    factory = null
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /** Create a RabbitMQ connection and receive data
    * until receiver is stopped
    */
  private def receive() {
    var connection : Connection = null
    var channel : Channel = null
    try {
      connection = factory.newConnection()
      channel = connection.createChannel()

      var queueName = props.get(RabbitMQReceiver.QueueNameKey).getOrElse("")
      // If no queue name is given, a fanout exchange name must be given.
      // We declare a exclusive, non-durable, auto-delete queue and bind the
      // queue to the fanout exchange
      // If the queue name is given, the queue must exist in the broker.
      if (queueName.isEmpty)  {
        val exchangeName = props.get(RabbitMQReceiver.ExchangeNameKey).get
        val declareOk = channel.queueDeclare();
        queueName = declareOk.getQueue()
        channel.queueBind(queueName, exchangeName, "")
      }
      logInfo(s"RabbitMQReceiver is started. Get msg from queue ${queueName}")

      props.get(RabbitMQReceiver.PrefetchCountKey).map(v => {
        var result :Int = 0
        try {
          result = v.toInt
        } catch {
          case e :NumberFormatException => {
            logWarning("Error when parsing" +
              s"${RabbitMQReceiver.PrefetchCountKey} value is ${v}", e)
          }
        }
        result
      }).map(v => {
        if (v > 0) {
          logInfo(s"Set channel prefetch count to ${v}.")
          channel.basicQos(v)
        }
      })


      val consumer = new QueueingConsumer(channel)
      // disable auto ack
      channel.basicConsume(queueName, false, consumer)

      while (!isStopped()) {
        try {
          val delivery = consumer.nextDelivery()
          val message = deserializer(delivery.getBody())
          if (isTraceEnabled())  {
            logTrace(s"Received '${message}'")
          }
          store(message)
          channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false)
        } catch {
          case _ :InterruptedException => {
            logWarning("consumer is interrupted during fetching messages.")
          }
        }
      }
    }
    catch {
      case cce :ConsumerCancelledException => {
        reportError("Consumer is cancelled. Receiver exits.", cce)
      }
      case sse :ShutdownSignalException => {
        reportError("Consumer Channel is shutdown. Receiver exits.", sse)
      }
      case ge :Exception => {
        reportError("Catch exception during fetch messages from RabbitMQ " +
          "broker.", ge)
        restart("try to restart the RabbitMQReceiver.", ge)
      }
    }
    finally {
      if (channel != null)  {
        channel.close()
      }
      if (connection != null)  {
        connection.close()
      }
    }
  }
}

object RabbitMQReceiver  {
  // all the default values for these props are inherited from RabbitMQ client
  // we donot set any default configuration value here
  val KeyPrefix = "spark.rabbitmq."
  val BrokerUriKey = s"${KeyPrefix}broker.uri"
  val BrokerHostKey = s"${KeyPrefix}broker.host"
  val BrokerPortKey = s"${KeyPrefix}broker.port"
  val UserKey = s"${KeyPrefix}user"
  val PasswdKey = s"${KeyPrefix}password"
  val VirtualHostKey = s"${KeyPrefix}virtualhost"
  val QueueNameKey = s"${KeyPrefix}queue.name"
  val ExchangeNameKey = s"${KeyPrefix}exchange.name"
  val PrefetchCountKey = s"${KeyPrefix}prefetch.count"
}
