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
import org.apache.spark.Logging
import org.apache.spark.streaming.receiver.Receiver

import scala.reflect.ClassTag

/**
 * Spark Input Dstream to pull messages from RabbitMQ broker
 * @param ssc_ spark streaming context
 * @param props configuration property map for RabbitMQ client setting
 * @param storageLevel storage level for streaming rdd
 */
class RabbitMQInputDStream[T :ClassTag](
    @transient ssc_ : StreamingContext,
    props : Map[String, String],
    deserializer : Array[Byte] => T,
    storageLevel : StorageLevel = StorageLevel.MEMORY_AND_DISK_2)
    extends ReceiverInputDStream[T](ssc_) with Logging {

  if (props.get(RabbitMQReceiver.QueueNameKey).getOrElse("").isEmpty &&
    props.get(RabbitMQReceiver.ExchangeNameKey).getOrElse("").isEmpty) {
    throw new IllegalArgumentException(s"Both ${RabbitMQReceiver.QueueNameKey} and" +
      s"${RabbitMQReceiver.ExchangeNameKey} are not set.")
  }

  def getReceiver() : Receiver[T] = {
    new RabbitMQReceiver(props, deserializer, storageLevel)
      .asInstanceOf[Receiver[T]]
  }
}
