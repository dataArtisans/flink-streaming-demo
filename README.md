## Demo Applications for Apache Flink&trade; DataStream

This repository contains demo applications for [Apache Flink](https://flink.apache.org)'s
[DataStream API](https://ci.apache.org/projects/flink/flink-docs-release-0.10/apis/streaming_guide.html).

Apache Flink is a scalable open-source streaming dataflow engine with many competitive features. <br>
You can find a list of Flink's features at the bottom of this page.

### Run a demo application in your IDE

You can run all examples in this repository from your IDE and play around with the code. <br>
Requirements:

- Java JDK 7 (or 8)
- Apache Maven 3.x
- Git
- an IDE with Scala support (we recommend IntelliJ IDEA)

To run a demo application in your IDE follows these steps:

1. **Clone the repository:** Open a terminal and clone the repository:
`git clone https://github.com/dataArtisans/flink-streaming-demo.git`. Please note that the
repository is about 100MB in size because it includes the input data of our demo applications.

2. **Import the project into your IDE:** The repository is a Maven project. Open your IDE and
import the repository as an existing Maven project. This is usually done by selecting the folder that
contains the `pom.xml` file or selecting the `pom.xml` file itself.

3. **Start a demo application:** Execute the `main()` method of one of the demo applications, for example
`com.dataartisans.flink_demo.examples.TotalArrivalCount.scala`.
Running an application will start a local Flink instance in the JVM process of your IDE.
You will see Flink's log messages and the output produced by the program being printed to the standard output.

4. **Explore the web dashboard:** The local Flink instance starts a webserver that serves Flink's
dashboard. Open [http://localhost:8081](http://localhost:8081) to access and explore the dashboard.

### Demo applications

#### Taxi event stream

All demo applications in this repository process a stream of taxi ride events that
originate from a [public data set](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)
of the [New York City Taxi and Limousine Commission](http://www.nyc.gov/html/tlc/html/home/home.shtml)
(TLC). The data set consists of records about taxi trips in New York City from 2009 to 2015.

We took some of this data and converted it into a data set of taxi ride events by splitting each
trip record into a ride start and a ride end event. The events have the following schema:

```
rideId: Long // unique id for each ride
time: DateTime // timestamp of the start/end event
isStart: Boolean // true = ride start, false = ride end
location: GeoPoint // lon/lat of pick-up/drop-off location
passengerCnt: short // number of passengers
travelDist: float // total travel distance, -1 on start events
```

A custom `SourceFunction` serves a `DataStream[TaxiRide]` from this data set.
In order to generate the stream as realistically as possible, events are emitted according to their
timestamp. Two events that occurred ten minutes after each other in reality are served ten minutes apart.
A speed-up factor can be specified to "fast-forward" the stream, i.e., with a speed-up factor of 2,
the events would be served five minutes apart. Moreover, you can specify a maximum serving delay
which causes each event to be randomly delayed within the bound to simulate an out-of-order stream
(a delay of 0 seconds results in an ordered stream). All examples operate in event-time mode.
This guarantees consistent results even in case of historic data or data which is delivered out-of-order.

#### Identify popular locations

The [`TotalArrivalCount.scala`](/src/main/scala/com/dataartisans/flink_demo/examples/TotalArrivalCount.scala)
program identifies popular locations in New York City.
It ingests the stream of taxi ride events and counts for each location the number of persons that
arrive by taxi.

#### Identify the popular locations of the last 15 minutes

The [`SlidingArrivalCount.scala`](/src/main/scala/com/dataartisans/flink_demo/examples/SlidingArrivalCount.scala)
program identifies popular locations of the last 15 minutes.
It ingests the stream of taxi ride records and computes every five minutes the number of
persons that arrived at each location within the last 15 minutes.
This type of computation is known as sliding window.


#### Compute early arrival counts for popular locations

Some stream processing use cases depend on timely event aggregation, for example to send out notifications or alerts.
The [`EarlyArrivalCount.scala`](/src/main/scala/com/dataartisans/flink_demo/examples/EarlyArrivalCount.scala)
program extends our previous sliding window application. Same as before, it computes every five minutes
the number of persons that arrived at each location within the last 15 minutes.
In addition it emits an early partial count whenever a multitude of 50 persons arrived at a
location, i.e., it emits an updated count if more than 50, 100, 150 (and so on) persons arrived at a location.

### Setting up Elasticsearch and Kibana

The demo applications in this repository are prepared to write their output to [Elasticsearch](https://www.elastic.co/products/elasticsearch).
Data in Elasticsearch can be easily visualized using [Kibana](https://www.elastic.co/products/kibana)
for real-time monitoring and interactive analysis.

Our demo applications depend on Elasticsearch 1.7.3 and Kibana 4.1.3. Both systems have a nice
out-of-the-box experience and operate well with their default configurations for our purpose.

Follow these instructions to set up Elasticsearch and Kibana.

#### Setup Elasticsearch

1. Download Elasticsearch 1.7.3 [here](https://www.elastic.co/downloads/past-releases/elasticsearch-1-7-3).

1. Extract the downloaded archive file and enter the extracted repository.

1. Start Elasticsearch using the start script: `./bin/elasticsearch`.

1. Create an index (here called `nyc-idx`): `curl -XPUT "http://localhost:9200/nyc-idx"`

1. Create a schema mapping for the index (here called `popular-locations`):
 ```
 curl -XPUT "http://localhost:9200/nyc-idx/_mapping/popular-locations" -d'
 {
  "popular-locations" : {
    "properties" : {
      "cnt": {"type": "integer"},
      "location": {"type": "geo_point"},
      "time": {"type": "date"}
    }
  }
 }'
 ```
 **Note:** This mapping can be used for all demo application.
1. Update a demo applications to write to Elasticsearch by changing the corresponding values in the
source code, i.e., set `writeToElasticsearch = true` and configure the correct host name (see Elasticsearch's log output).

1. Run the Flink program to write its result to Elasticsearch.

To clear the `nyc-idx` index in Elasticsearch, simply drop the mapping as
`curl -XDELETE 'http://localhost:9200/nyc-idx/popular-locations'` and create it again with the previous
command.

#### Setup Kibana

Setting up Kibana and visualizing data that is stored in Elasticsearch is also easy.

1. Dowload Kibana 4.1.3 [here](https://www.elastic.co/downloads/past-releases/kibana-4-1-3)

1. Extract the downloaded archive and enter the extracted repository.

1. Start Kibana using the start script: `./bin/kibana`.

1. Access Kibana by opening [http://localhost:5601](http://localhost:5601) in your browser.

1. Configure an index pattern by entering the index name "nyc-idx" and clicking on "Create".
Do not uncheck the "Index contains time-based events" option.

1. Select the time range to visualize with Kibana. Click on the "Last 15 minutes" label in the 
top right corner and enter an absolute time range from 2013-01-01 to 2013-01-06 which is the time
range of our taxi ride data stream. You can also configure a refresh interval to reload the page
for updates.

1. Click on the “Visualize” button at the top of the page, select "Tile map", and click on "From a
new search".

1. Next you need to configure the tile map visualization:

  - Top-left: Configure the displayed value to be a “Sum” aggregation over the "cnt" field.
  - Top-left: Select "Geo Coordinates" as bucket type and make sure that "location" is
configured as field.
  - Top-left: You can change the visualization type by clicking on “Options” (top left) and selecting
for example a “Shaded Geohash Grid” visualization.
  - The visualization is started by clicking on the green play button.

The following screenshot shows how Kibana visualizes the result of `TotalArrivalCount.scala`.

![Kibana Screenshot](/data/kibana.jpg?raw=true "Kibana Screenshot")

### Apache Flink's Feature Set

- **Support for out-of-order streams and event-time processing**: In practice, streams of events rarely
arrive in the order that they are produced, especially streams from distributed systems, devices, and sensors.
Flink 0.10 is the first open source engine that supports out-of-order streams and event
time which is a hard requirement for many application that aim for consistent and meaningful results.

- **Expressive and easy-to-use APIs in Scala and Java**: Flink's DataStream API provides many
operators which are well known from batch processing APIs such as `map`, `reduce`, and `join` as
well as stream specific operations such as `window`, `split`, and `connect`.
First-class support for user-defined functions eases the implementation of custom application
behavior. The DataStream API is available in Scala and Java.

- **Support for sessions and unaligned windows**: Most streaming systems have some concept of windowing,
i.e., a temporal grouping of events based on some function of their timestamps. Unfortunately, in
many systems these windows are hard-coded and connected with the system’s internal checkpointing
mechanism. Flink is the first open source streaming engine that completely decouples windowing from
fault tolerance, allowing for richer forms of windows, such as sessions.

- **Consistency, fault tolerance, and high availability**: Flink guarantees consistent operator state
in the presence of failures (often called "exactly-once processing"), and consistent data movement
between selected sources and sinks (e.g., consistent data movement between Kafka and HDFS). Flink
also supports master fail-over, eliminating any single point of failure.

- **High throughput and low-latency processing**: We have clocked Flink at 1.5 million events per second per core,
and have also observed latencies at the 25 millisecond range in jobs that include network data
shuffling. Using a tuning knob, Flink users can control the latency-throughput trade-off, making
the system suitable for both high-throughput data ingestion and transformations, as well as ultra
low latency (millisecond range) applications.

- **Integration with many systems for data input and output**: Flink integrates with a wide variety of
open source systems for data input and output (e.g., HDFS, Kafka, Elasticsearch, HBase, and others),
deployment (e.g., YARN), as well as acting as an execution engine for other frameworks (e.g.,
Cascading, Google Cloud Dataflow). The Flink project itself comes bundled with a Hadoop MapReduce
compatibility layer, a Storm compatibility layer, as well as libraries for Machine Learning and
graph processing.

- **Support for batch processing**: In Flink, batch processing is a special case of stream processing,
as finite data sources are just streams that happen to end. Flink offers a dedicated execution mode
for batch processing with a specialized DataSet API and libraries for Machine Learning and graph processing. In
addition, Flink contains several batch-specific optimizations (e.g., for scheduling, memory
management, and query optimization), matching and even out-performing dedicated batch processing
engines in batch use cases.

- **Developer productivity and operational simplicity**: Flink runs in a variety of environments. Local
execution within an IDE significantly eases development and debugging of Flink applications.
In distributed setups, Flink runs at massive scale-out. The YARN mode
allows users to bring up Flink clusters in a matter of seconds. Flink serves monitoring metrics of
jobs and the system as a whole via a well-defined REST interface. A build-in web dashboard
displays these metrics and makes monitoring of Flink very convenient.


<hr>

<center>
Copyright &copy; 2015 dataArtisans. All Rights Reserved.

Apache Flink, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.
</center>