package com.order.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * This deserializer file is used to perform custom operations such as conversion from
 * compressed payloads to JSON before sending these payloads to the internal
 * operators within Flink
 */

public class DeserializerSchema implements DeserializationSchema<String> {

    private static final Logger logger = LoggerFactory.getLogger(DeserializerSchema.class);

    @Override
    public String deserialize(byte[] bytes) throws IOException {
        try {

            /**
             * This block decompresses a GZIP payload. Some external source streams send compressed payloads
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

            // decode base64 encoded payload
            byte[] decodedBytes = Base64.getDecoder().decode(bytes);

            // Convert the decoded bytes to a string
            return new String(decodedBytes, StandardCharsets.UTF_8);
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