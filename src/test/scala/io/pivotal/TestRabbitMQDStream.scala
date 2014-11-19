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

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.{Logging}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.rabbitmq.client.{Connection, Channel, ConnectionFactory}


class RabbitMQStreamTestSuite extends FunSuite
    with BeforeAndAfter with Logging  {
  val host = "localhost"
  val portStr = "5672"
  val portInt = 5672
  val uri = "amqp://localhost:5672"
  val exchange = "rabbitdstream"
  val queue = "testQ"
  val master = "local[2]"
  val framework = this.getClass.getSimpleName

  var pcon : Option[Connection] = None
  var pchannel : Option[Channel] = None

  val messages = Array("rabbit stream", "spark stream", "stream test",
    "spark   stream   test")
  // expected test result
  val expectedResult = Map("rabbit" -> 1, "spark" -> 2, "stream" -> 4,
    "test" -> 2)
  val expectedRecordCount = 4

  def setupTest() {
    val cfactory = new ConnectionFactory()
    cfactory.setHost(host)
    cfactory.setPort(portInt)
    pcon = Some(cfactory.newConnection())
    if (pcon.isDefined) {
      pchannel = Some(pcon.get.createChannel())
    }
    pchannel.foreach(_.exchangeDeclare(exchange, "fanout", true))
    pchannel.foreach(_.queueDeclare(queue, false, false, false, null))
    pchannel.foreach(_.queueBind(queue, exchange, ""))
  }

  def tearDownTest()  {
    pchannel.foreach((c :Channel) => {
      c.queueUnbind(queue, exchange, "")
      c.queueDelete(queue)
      c.exchangeDelete(exchange)
    })
    pchannel.foreach(_.close())
    pcon.foreach(_.close())
    pchannel = None
    pcon = None
  }

  def publishMessages() =  {
    pchannel.foreach((channel : Channel) => {
    messages.foreach((msg: String) => {
      logDebug(s"publish message ${msg} to exchange ${exchange}")
      channel.basicPublish(exchange, "", null, msg.getBytes())
    })
    })
  }

  before(setupTest)

  after(tearDownTest)

  test("test create rabbitmq dstream api") {
    def desfun(bytes :Array[Byte]) :Int = {
      val str = new String(bytes)
      str.toInt
    }

    val ssc = new StreamingContext(master, framework, Seconds(1))
    val s1 = RabbitMQUtil.createStrStream(ssc, host, portInt, queue)
    val s11 = RabbitMQUtil.createStream[Int](ssc, desfun, host, portInt, queue)
    val s2 = RabbitMQUtil.createStrStream(ssc, host, portInt, queue,
        user = "spark", password = "1234", prefetch = 10)
    val s22 = RabbitMQUtil.createStream[Int](ssc, desfun, host, portInt,
        queue, user = "spark", password = "321", prefetch = 100)
    val s3 = RabbitMQUtil.createStrStream(ssc, host, portInt, queue,
        prefetch = 100)
    val s33 = RabbitMQUtil.createStream[Int](ssc, desfun, host, portInt, queue,
      prefetch = 3)
    val s4 = RabbitMQUtil.createStrStreamWithUri(ssc, uri, queue,
      storageLevel = StorageLevel.MEMORY_AND_DISK_SER_2)
    val s44 = RabbitMQUtil.createStreamWithUri[Int](ssc, desfun, uri, queue,
      storageLevel = StorageLevel.MEMORY_AND_DISK_SER_2)
    var conf = Map(RabbitMQReceiver.BrokerHostKey -> host,
        RabbitMQReceiver.BrokerPortKey -> portStr)
    intercept[IllegalArgumentException] {
      val s5 = RabbitMQUtil.createStrStreamWithConf(ssc, conf,
        StorageLevel.MEMORY_ONLY)
    }
    intercept[IllegalArgumentException] {
      val s55 = RabbitMQUtil.createStreamWithConf[Int](ssc, desfun, conf,
        StorageLevel.MEMORY_ONLY)
    }
    conf += (RabbitMQReceiver.ExchangeNameKey -> exchange)
    val s6 = RabbitMQUtil.createStrStreamWithConf(ssc, conf,
      StorageLevel.MEMORY_ONLY)
    val s66 = RabbitMQUtil.createStreamWithConf[Int](ssc, desfun, conf,
      StorageLevel.MEMORY_ONLY)
  }

  test("test rabbitmq dstream with given queue name")  {
    val ssc = new StreamingContext(master, framework, Seconds(1))
    val s = RabbitMQUtil.createStrStream(ssc, host, portInt, queue)

    val result = scala.collection.mutable.HashMap[String, Int]()
    val wordcount = s.flatMap(_.split("[ ]+")).map( x => (x,
      1)).reduceByKey(_ + _)
    wordcount.foreachRDD( r => {
      val m = r.collect().toMap
      m.foreach(result += _)
    }
    )

    ssc.start()
    // queue is setup, send msgs now
    publishMessages()
    ssc.awaitTermination(5000)

    val recordCount = result.size
    logInfo(s"Get result ${result}")
    assert(expectedRecordCount === recordCount)
    result.foreach((entry : (String, Int)) => {
      val r = expectedResult.get(entry._1).getOrElse(-1)
      assert(r === entry._2)
    })

    ssc.stop()
  }

  test("test rabbitmq dstream with given exchange")  {
    val ssc = new StreamingContext(master, framework, Seconds(1))
    val s = RabbitMQUtil.createStrStream(ssc, host, portInt, "",
      exchange = exchange, prefetch = 1)

    val result = scala.collection.mutable.HashMap[String, Int]()
    val wordcount = s.flatMap(_.split("[ ]+")).map( x => (x,
      1)).reduceByKey(_ + _)
    wordcount.foreachRDD( r => {
      val m = r.collect().toMap
      m.foreach(result += _)
    }
    )
    ssc.start()
    // sleep 3 seconds, waiting for the queue is setup
    Thread.sleep(3000)
    publishMessages()
    ssc.awaitTermination(5000)

    val recordCount = result.size
    assert(expectedRecordCount === recordCount)
    result.foreach((entry : (String, Int)) => {
      val r = expectedResult.get(entry._1).getOrElse(-1)
      assert(r === entry._2)
    })

    ssc.stop()
  }

  test("test rabbitmq dstream exception case")  {
    val ssc = new StreamingContext(master, framework, Seconds(1))
    intercept[IllegalArgumentException] {
      val s = RabbitMQUtil.createStrStream(ssc, host, portInt, "")
    }
  }

//  def main(args: Array[String])  {
//    if (args.length < 1) {
//      System.err.println("no hostname is found.")
//      System.exit(1)
//      return;
//    }
//
//    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
//    val ssc = new StreamingContext(sparkConf, Seconds(3))
//
//    val lines = ssc.receiverStream(new RabbitMQReceiver(args
//      (0), "hello"))
//    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
//    wordCounts.print()
//    ssc.start()
//    ssc.awaitTermination()
//  }
}
