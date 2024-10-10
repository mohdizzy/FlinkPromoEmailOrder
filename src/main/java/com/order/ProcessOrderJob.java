package com.order;

import com.order.deserializers.DeserializerSchema;
import com.order.functions.OrderProcessorFunction;
import com.order.serializers.OutputSchemaProducer;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class ProcessOrderJob
{
	private static final ObjectMapper mapper = new ObjectMapper();
	private static final Logger logger = LoggerFactory.getLogger(ProcessOrderJob.class);

	public static void main(String[] args) throws Exception
	{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// AWS KDA allows you to define properties map where you can setup the config values
		Map<String, Properties> properties = KinesisAnalyticsRuntime.getApplicationProperties();

		String bootstrapServers = properties.get("bootstrap").getProperty("bootstrap.servers");

		// Setup properties at AWS KDA console (or through IaC code deployment)
		// Get the source topic name, group ID from the properties map under "KafkaSourceOrderStream"
		String sourceTopic = properties.get("KafkaSourceOrderStream").getProperty("topic");
		String consumerGroupId = properties.get("KafkaSourceOrderStream").getProperty("consumerGroupId");
		// Get the sink topic name from the properties map under "KafkaOrderSink"
		String processedOrderSinkTopic = properties.get("KafkaOrderSink").getProperty("topic");

		/**
		 * Properties section for each sink topic can contain these elements,
		 * bootstrap.servers: KAFKA BROKER CONNECTION STRINGS
		 * security.protocol: SSL
		 * group.id: orderGroup (this can be anything that identifies your consumer within Kafka)
		 * topic: Source topic name
		 * flink.partition-discovery.interval-millis: 10000
		 */

		/**
		 * Properties section for each source topic can contain these elements,
		 * bootstrap.servers: KAFKA BROKER CONNECTION STRINGS
		 * security.protocol: SSL
		 * topic: Sink topic name
		 * transaction.timeout.ms: 900000
		 */

		// Connector for processing the incoming source stream
		// You can specify a default deserializer or setup a custom one, here we have a custom one
		KafkaSource<String> ordersSource = KafkaSource.<String>builder().setBootstrapServers(bootstrapServers)
				.setTopics(sourceTopic).setGroupId(consumerGroupId)
				.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new DeserializerSchema()))
				.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)).build();

		// Connector for pushing the processed events from Flink to destination topic
		// You can specify a default serializer or setup a custom one, here we have a custom one
		Properties sinkProps = new Properties();
		sinkProps.setProperty("bootstrap.servers", bootstrapServers);
		sinkProps.setProperty("transaction.timeout.ms", "90000"); // can be defined under Properties section
		KafkaSink<String> ordersSink = KafkaSink.<String>builder().setKafkaProducerConfig(properties.get("KafkaOrderSink"))
				.setRecordSerializer(new OutputSchemaProducer(processedOrderSinkTopic)).setTransactionalIdPrefix("order-events")
				.build();
		
		// Setup of the stream that will initiate the processing. We chain the required operators here
		SingleOutputStreamOperator<String> orderStream = env.fromSource(ordersSource, WatermarkStrategy.noWatermarks(), "order-event-source")
				.keyBy(input -> {
					try
					{
						// Refer the sample order_event json under test folder
						// Here we key the stream based on the customerNumber
						return mapper.readTree(input).get("customer").get("customerNumber").asText();
					}
					catch (Exception e){
						// Tip: log failures where necessary (setup your own logger & exception class)
						logger.info("Cannot perform keyBy");
						return "";
					}
				})
				.process(new OrderProcessorFunction()).uid("order-processor");

		// Specify the "sink" for the above stream so Flink knows where to collect the events
		orderStream.sinkTo(ordersSink).uid("orders-sink").name("Processed order event sink");

		env.execute("Order processing job");
	}

}
