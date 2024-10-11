package com.order.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.order.utils.JsonpathExceptionHandler;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;

import java.util.List;
import java.util.Map;

public class OrderProcessorFunction extends KeyedProcessFunction<String, String, String>
{
	private static final ObjectMapper mapper = new ObjectMapper();

	private static final Logger logger = LoggerFactory.getLogger(OrderProcessorFunction.class);

	// state variable that keeps track when specific product IDs have come in
	private ValueState<Boolean> productOneReceived;

	private ValueState<Boolean> productTwoReceived;

	@Override
	public void open(org.apache.flink.configuration.Configuration parameters) throws Exception
	{
		super.open(parameters);
		ValueStateDescriptor<Boolean> productOneStateDescriptor = new ValueStateDescriptor<>("productOneState",
				Boolean.class);
		ValueStateDescriptor<Boolean> productTwoStateDescriptor = new ValueStateDescriptor<>("productTwoState",
				Boolean.class);
		productOneReceived = getRuntimeContext().getState(productOneStateDescriptor);
		productTwoReceived = getRuntimeContext().getState(productTwoStateDescriptor);
	}

	@Override
	public void processElement(String input, Context ctx, Collector<String> out) throws Exception
	{
		Object document = Configuration.defaultConfiguration().jsonProvider().parse(input);

		// 542,723 are the specific Product IDs we are looking to check
		// Jsonpath is the library being used to search for specific parameters within a JSON
		List<Map<String, Object>> productOneCheck = JsonpathExceptionHandler.getList(document,
				"$.products[?(@.sku=='542')].sku");
		List<Map<String, Object>> productTwoCheck = JsonpathExceptionHandler.getList(document,
				"$.products[?(@.sku=='723')].sku");

		if (!productOneCheck.isEmpty())
		{
			logger.info("Received product ID 542");
			productOneReceived.update(true); // set the state variable a match for product ID one is found
		}
		if (!productTwoCheck.isEmpty())
		{
			logger.info("Received product ID 723");
			productTwoReceived.update(true); // set the state variable a match for product ID two is found
		}

		// create a timer when at least one of the product IDs have been detected, this will clear state at end of day
		if (productOneReceived.value() != null || productTwoReceived.value() != null)
		{
			ZoneId zoneId = ZoneId.systemDefault();
			ZonedDateTime now = ZonedDateTime.now(zoneId);
			ZonedDateTime endOfDay = now.toLocalDate().atTime(LocalTime.MAX).atZone(zoneId);
			long millisLeftForEndOfDay = endOfDay.toInstant().toEpochMilli() - now.toInstant().toEpochMilli();
			// sets a timer to trigger at end of day for clearing state
			ctx.timerService().registerProcessingTimeTimer(Instant.now().toEpochMilli() + millisLeftForEndOfDay);
		}

		// send output from flink to kafka topic only when both products have been received for a given customer
		// values for Valuestate without initialization are null by default, hence the additional check
		if (productOneReceived.value() != null && productTwoReceived.value() != null && productOneReceived.value()
				&& productOneReceived.value())
			out.collect(input); // this "collects" the output to the sink OR to the next operator chained in the Job class

		// to avoid sending emails if anyone of the two products are ordered again, an additional state flag can be used
		// to block sending any outputs from flink
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx,
			Collector<String> out) throws Exception
	{
		logger.info("Clearing state at end of day");
		productOneReceived.clear();
		productTwoReceived.clear();
	}
}

