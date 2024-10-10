package com.order.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;


public class OutputSchemaProducer
        implements KafkaRecordSerializationSchema<String>
{
    private String topic;
    private static final Logger logger = LoggerFactory.getLogger(OutputSchemaProducer.class);
    private ObjectMapper mapper;

    public OutputSchemaProducer(String topic)
    {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String payload, KafkaRecordSerializationSchema.KafkaSinkContext kafkaSinkContext, Long timestamp)
    {
        byte[] value = null;
        byte[] key = null;
        if (mapper == null)
        {
            mapper = new ObjectMapper();
        }
        try
        {
            // send the payloads Flink to Kafka with a key so that they are ordered within the partitions for that key
            key = mapper.readTree(payload).get("customer").get("customerNumber").asText().getBytes(StandardCharsets.UTF_8);
            value = mapper.writeValueAsBytes(payload);
        }
        catch (Exception e)
        {
            logger.info("Could not send payload to kafka");
        }
        return new ProducerRecord<>(topic, key, value);
    }
}