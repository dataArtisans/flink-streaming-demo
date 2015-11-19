/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink_demo.sinks

import java.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress

import scala.collection.JavaConversions._

/**
 * SinkFunction to either insert or update an entry in an Elasticsearch index.
 *
 * @param host Hostname of the Elasticsearch instance.
 * @param port Port of the Elasticsearch instance.
 * @param cluster Name of the Elasticsearch cluster.
 * @param index Name of the Elasticsearch index.
 * @param mapping Name of the index mapping.
 *
 * @tparam T Record type to write to Elasticsearch.
 */
abstract class ElasticsearchUpsertSink[T](host: String, port: Int, cluster: String, index: String, mapping: String)
  extends RichSinkFunction[T] {

  private var client: TransportClient = null

  def insertJson(record: T): Map[String, AnyRef]

  def updateJson(record: T): Map[String, AnyRef]

  def indexKey(record: T): String

  @throws[Exception]
  override def open(parameters: Configuration) {

    val config = new util.HashMap[String, String]
    config.put("bulk.flush.max.actions", "1")
    config.put("cluster.name", cluster)

    val settings = ImmutableSettings.settingsBuilder()
      .put(config)
      .build()
    client = new TransportClient(settings)
      .addTransportAddress(new InetSocketTransportAddress(host, port))
  }

  @throws[Exception]
  override def invoke(r: T) {
    // do an upsert request to elastic search

    // index document if it does not exist
    val indexRequest = new IndexRequest(index, mapping, indexKey(r))
      .source(mapAsJavaMap(insertJson(r)))

    // update document if it exists
    val updateRequest = new UpdateRequest(index, mapping, indexKey(r))
      .doc(mapAsJavaMap(updateJson(r)))
      .upsert(indexRequest)

    client.update(updateRequest).get()
  }

}
