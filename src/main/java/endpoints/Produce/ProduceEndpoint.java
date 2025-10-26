package endpoints.Produce;

import api.RequestBody;
import endpoints.Fetch.models.FetchRequestBody;
import endpoints.Fetch.models.FetchRequestTopic;
import endpoints.KafkaEndpoint;
import endpoints.Produce.models.ProduceRequestBody;
import endpoints.Produce.models.ProduceRequestPartition;
import endpoints.Produce.models.ProduceRequestTopic;
import utils.ConstructorException;
import utils.ErrorCodes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import static utils.NumbersUtils.*;

public class ProduceEndpoint implements KafkaEndpoint {
    @Override
    public void process(RequestBody requestBody, ByteArrayOutputStream responseBuffer) throws IOException, ConstructorException {
        System.out.println("Handling Produce request");
        byte tag_buffer = 0;
        ByteArrayInputStream bodyStream = new ByteArrayInputStream(requestBody.body);
        // Parse request body
        ProduceRequestBody produceRequestBody = new ProduceRequestBody(bodyStream);
        // Write response
        System.out.println("Writing Produce response");
        // Response Header
        responseBuffer.write(requestBody.input_correlation_id);
        responseBuffer.write(tag_buffer);
        // Response Body
        writeResponseBody(produceRequestBody, responseBuffer);
    }

    private void writeResponseBody(ProduceRequestBody requestBody, ByteArrayOutputStream responseBuffer) throws IOException {
        byte[] errorCode = shortToBytes((short) ErrorCodes.NO_ERROR);
        byte tag_buffer = 0;
        // Topics Array
        responseBuffer.write(encodeVarInt(requestBody.topicsArrayLength));
        if(requestBody.topicsArrayLength>1) {
            for(ProduceRequestTopic topic : requestBody.topics) {
                System.out.println("Writing for topic name " + Arrays.toString(topic.topicName));
                // Topic Name
                responseBuffer.write(encodeVarInt(topic.topicNameLength));
                responseBuffer.write(topic.topicName);
                // Partitions Array
                responseBuffer.write(encodeVarInt(topic.partitionsArrayLength));
                for(ProduceRequestPartition partition : topic.partitionsArray) {
                    // TODO handle partitions response successfully
                    responseBuffer.write(partition.partitionId);
                    // TODO This is hardcoded for now, we should detect real error
                    responseBuffer.write(shortToBytes((short) ErrorCodes.UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE));
                    // Base offset
                    byte[] baseOffset = new byte[8];
                    responseBuffer.write(baseOffset);
                    // Log append time
                    byte[] logAppendTimeMs = new byte[8];
                    responseBuffer.write(logAppendTimeMs);
                    // log_start_offset
                    byte[] logStartOffset = new byte[8];
                    responseBuffer.write(logStartOffset);
                    // Record Errors Array Length
                    responseBuffer.write(0);
                    // Error Message
                    responseBuffer.write(0);
                    // Tag Buffer
                    responseBuffer.write(tag_buffer);
                }

                // Tag Buffer
                responseBuffer.write(tag_buffer);
            }
        }
        byte[] throttle_time_ms = intToBytes(0);
        responseBuffer.write(throttle_time_ms);
        // Tag Buffer
        responseBuffer.write(tag_buffer);
    }
}
