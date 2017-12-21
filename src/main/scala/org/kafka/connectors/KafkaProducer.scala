package org.kafka.connectors

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

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.serialization.SimpleStringSchema


object KafkaProducer {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // set properties to connect to kafka
    val kafkaProperties = new Properties()
    kafkaProperties.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProperties.setProperty("batch.size", "1000000")

    val stream = env.addSource(new TimeStampGenerator())
    stream.addSink(new FlinkKafkaProducer010[String]("test-01", new SimpleStringSchema(), kafkaProperties))

    env.execute("Write to Kafka")
  }

  class TimeStampGenerator() extends SourceFunction[String] {
    var running = true

    override def cancel(): Unit = {
      running = false
    }

    override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
      while(running) {
        val ts = System.currentTimeMillis()
        sourceContext.collect(ts.toString + ",")
        Thread.sleep(0, 1000)
      }
    }
  }
}
