package endpoints.DescribeTopic;

import static utils.Utils.bytesToInt;
import static utils.Utils.intToBytes;
import static utils.Utils.shortToBytes;
import utils.ErrorCodes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Arrays;

import endpoints.KafkaEndpoint;
import api.RequestBody;

public class DescribeTopicEndpoint implements KafkaEndpoint {;

    ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();

    @Override
    public void process(RequestBody requestBody, OutputStream outputStream) throws IOException {
        responseBuffer.reset();
        // Only support 0-0 versions
        if (bytesToInt(requestBody.input_request_api_version) >= 0 && bytesToInt(requestBody.input_request_api_version) <= 0) {
            System.out.println("Handling a proper request");
            // Response Header
                byte tag_buffer = 0;
                responseBuffer.write(requestBody.input_correlation_id);
                responseBuffer.write(tag_buffer);
            // Throttle time
                byte[] throttle_time_ms = intToBytes(100);
                responseBuffer.write(throttle_time_ms);
            // Topics Array
                ByteArrayInputStream bodyStream = new ByteArrayInputStream(requestBody.body);
            // Length of array
                byte[] input_topics_array_size = new byte[1]; 
                bodyStream.read(input_topics_array_size);
                responseBuffer.write(input_topics_array_size);
            // Topics Array
                byte[][] input_topics_names = new byte[(input_topics_array_size[0])-1][];

                for(int i=0; i<bytesToInt(input_topics_array_size); i++) {
                    input_topics_names[i] = readTopicName(bodyStream);
                }

                byte[] input_response_partition_limit = new byte[4];
                bodyStream.read(input_response_partition_limit);
                byte[] input_pagination_tag = new byte[1];
                bodyStream.read(input_pagination_tag);
                // Tag Buffer
                bodyStream.read();
                responseBuffer.write(generateTopicsArrayResponse(input_topics_names));
            // Next Cursor
                responseBuffer.write(255);
            // Tag Buffer
                responseBuffer.write(tag_buffer);
        } else {
            // Throw appropriate error code
            System.out.println("Handling a wrong request");
            byte[] errorCode = shortToBytes((short) ErrorCodes.WRONG_REQUEST_ERROR_CODE);
            // specifies the size of the header and body.
            responseBuffer.write(requestBody.input_correlation_id);
            responseBuffer.write(errorCode);
        }
        byte[] responseBytes = responseBuffer.toByteArray();
        // send data to client
        outputStream.write(intToBytes(responseBytes.length));
        outputStream.write(responseBytes);
        outputStream.flush();
    }

    private byte[] readTopicName(ByteArrayInputStream topic) throws IOException{
        byte[] topic_name_length = new byte[1];
        topic.read(topic_name_length);
        byte[] topic_name = new byte[bytesToInt(topic_name_length)-1];
        topic.read(topic_name);
        // Tag Buffer
        topic.read();
        return topic_name;
    }

    private byte[] generateTopicsArrayResponse(byte[][] input_topics_names) throws IOException{
        ByteArrayOutputStream topicsArrayBuffer = new ByteArrayOutputStream();
        byte tag_buffer = 0;
        for(int i=0; i<input_topics_names.length; i++) {
            // Error Code
            // TODO handle proper error code, instead of unknown topic
            topicsArrayBuffer.write(shortToBytes(ErrorCodes.UNKOWN_TOPIC_ERROR_CODE));
            // Topic name
            topicsArrayBuffer.write(input_topics_names[i].length+1);
            topicsArrayBuffer.write(input_topics_names[i]);
            // Topic ID
            // TODO Handle real topic ID
            byte[] topicId = new byte[16];
            Arrays.fill(topicId, (byte) 0);
            topicsArrayBuffer.write(topicId);
            // is Internal topic
            topicsArrayBuffer.write(0);
            // partitions array length
            // TODO handle partitions
            topicsArrayBuffer.write(1);
            //Topic Authorized Operations
            // TODO handle it properly, instead of hardcoded value
            byte[] authorizedOperations = {0,0,0,0,1,1,0,1,1,1,1,1,1,0,0,0};
            topicsArrayBuffer.write(authorizedOperations);
            //tag buffer
            topicsArrayBuffer.write(tag_buffer);
        }
        return topicsArrayBuffer.toByteArray();
    }
}
