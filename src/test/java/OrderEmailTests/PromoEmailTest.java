package OrderEmailTests;

import com.order.deserializers.DeserializerSchema;
import com.order.functions.OrderProcessorFunction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * This test class contains two kinds of tests
 * 1. testFlinkJob() uses some utils from Flink framework and specifically tests the operators associated with your datastreams.
 * Some functionality like 'timers' will not work here.
 * 2. endToEndTest() emulates the whole Flink framework just like how it would work in real time.
 */

public class PromoEmailTest
{
	private static final ObjectMapper mapper = new ObjectMapper();
	private static final Logger logger = LoggerFactory.getLogger(PromoEmailTest.class);

	@Test
	void testFlinkJob() throws Exception
	{



		OrderProcessorFunction processor = new OrderProcessorFunction();

		KeyedProcessOperator<String, String, String> keyProcessor = new KeyedProcessOperator<>(processor);
		KeyedOneInputStreamOperatorTestHarness<String, String,String> harness = new KeyedOneInputStreamOperatorTestHarness<>(
				keyProcessor, orderInput -> {
			try
			{
				return mapper.readTree(orderInput).get("customer").get("customerNumber").asText();
			}
			catch (Exception e)
			{
				System.out.println(e);
				logger.info("cannot parse json");
				return "";
			}
		},TypeInformation.of(String.class));
		harness.open();

		harness.processElement(createOrderEvent("/src/test/java/events/order_event1.txt"), Instant.now().toEpochMilli());
		harness.processElement(createOrderEvent("/src/test/java/events/order_event2.txt"), Instant.now().toEpochMilli());

		List<StreamRecord<? extends String>> records = harness.extractOutputStreamRecords();
		assertThat(records.size()).isEqualTo(1);
		System.out.println(mapper.writeValueAsString(records.get(0)));
	}

	private String createOrderEvent(String filePath) throws Exception
	{
		String stringPath = new File("./").getCanonicalPath();
		stringPath += filePath;
		String orderEvent = new String(Files.readAllBytes(Paths.get(stringPath)));
		DeserializerSchema deserializerSchema = new DeserializerSchema();
		return deserializerSchema.deserialize(orderEvent.getBytes());
	}

	static List<String> output = new ArrayList<>();

	@Test
	void endToEndTest() throws Exception
	{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		SinkFunction<String> fakeSink = new SinkFunction<String>()
		{
			@Override
			public void invoke(String value, Context context) throws Exception
			{
				output.add(value);
			}
		};

		buildFlinkJobCore(env, orderFakeSource(), fakeSink);
		env.execute();
		String jsonFromKafka = new ObjectMapper().writeValueAsString(output);
		System.out.println("Flink Output: " + jsonFromKafka);
		assertThat(output.size()).isEqualTo(1);
	}

	private void buildFlinkJobCore(StreamExecutionEnvironment env, SourceFunction<String> orderSource,
			SinkFunction<String> sink)
	{

		SingleOutputStreamOperator<String> mainDataStream = env.addSource(orderSource)
				.keyBy(input -> {
					try
					{
						// Refer the sample order_event json under test folder
						// Here we key the stream based on the customerNumber
						return mapper.readTree(input).get("customer").get("customerNumber").asText();
					}
					catch (Exception e){

						return "";
					}
				})
				.process(new OrderProcessorFunction());
		mainDataStream.addSink(sink);
		mainDataStream.print();


	}

	private static SourceFunction<String> orderFakeSource()
	{
		return new SourceFunction<String>()
		{
			@Override
			public void run(SourceContext<String> ctx) throws Exception
			{
				int count = 0;
				while (count < 2)
				{
					String deserializedString = "";
					String inputXML = "";
					String xmlPath = new File("./").getCanonicalPath();
					DeserializerSchema deserializerSchema = new DeserializerSchema();

					if (count == 0)
					{
						xmlPath += "/src/test/java/events/order_event1.txt";
						inputXML = new String(Files.readAllBytes(Paths.get(xmlPath)));
						deserializedString = deserializerSchema.deserialize(inputXML.getBytes());
					}

					if (count == 1)
					{
						xmlPath += "/src/test/java/events/order_event2.txt";
						inputXML = new String(Files.readAllBytes(Paths.get(xmlPath)));
						deserializedString = deserializerSchema.deserialize(inputXML.getBytes());
					}


					ctx.collect(deserializedString);
					count++;
				}
			}

			@Override
			public void cancel()
			{
			}
		};
	}
}
