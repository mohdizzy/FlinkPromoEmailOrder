package com.order.deserializers;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * This deserializer file is used to perform custom operations such as conversion from
 * compressed payloads to JSON before sending these payloads to the internal
 * operators within Flink
 */

public class DeserializerSchema implements DeserializationSchema<String> {

    private static final Logger logger = LoggerFactory.getLogger(DeserializerSchema.class);
    static final String DESERIALIZATION = "DESERIALIZATION";
    @Override
    public String deserialize(byte[] bytes) throws IOException {
        try {

            /**
             * This block decompresses a GZIP payload. Usually some external source streams send compressed payloads
             * when they are being routed through the internet for better network performance
             */
//            byte[] compressed = Base64.getDecoder().decode(new String(bytes, StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8));
//            StringBuilder outStr = new StringBuilder();
//            GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(compressed));
//            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gis, StandardCharsets.UTF_8));
//            String line;
//            while ((line = bufferedReader.readLine()) != null) {
//                outStr.append(line);
//            }
//            // To convert byte to string
//            String inputJsonString = outStr.toString();

            // If no decompression is required, just return payload string
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (Exception e) {
            logger.error("Failed deserialization event");
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(String inputMessage) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}