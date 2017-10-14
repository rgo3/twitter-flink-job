package org.hackupc.twitter.analysis

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer082}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema


object TweetCountKafkaConsumer {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // set properties to connect to kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "54.218.59.178:9092,54.186.93.122:9092")
    properties.setProperty("group.id", "test")
    val stream = env
      .addSource(new FlinkKafkaConsumer010[String]("tweets-test", new SimpleStringSchema(), properties))
    // create streams for names and ages by mapping the inputs to the corresponding objects
    stream.printToErr()


    env.execute("Scala TweetCount from KafkaSource")
  }
}
