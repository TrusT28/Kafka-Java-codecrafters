package endpoints.DescribeTopic;

import static utils.Utils.bytesToInt;
import static utils.Utils.intToBytes;
import static utils.Utils.shortToBytes;
import utils.ErrorCodes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import endpoints.KafkaEndpoint;
import endpoints.DescribeTopic.models.MetadataBatches;
import api.RequestBody;

public class DescribeTopicEndpoint implements KafkaEndpoint {;

    ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
    ClusterMetadataReader clusterMetadataReader = new ClusterMetadataReader();

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
                byte[] throttle_time_ms = intToBytes(0);
                responseBuffer.write(throttle_time_ms);
            // Topics Array
                System.out.println("Request Body body length is now" + requestBody.body.length);
                ByteArrayInputStream bodyStream = new ByteArrayInputStream(requestBody.body);
                System.out.println("bodyStream is now" + bodyStream.available());
            // Length of array
                byte[] input_topics_array_size = new byte[1];
                System.out.println("Fails now?");
                bodyStream.read(input_topics_array_size);
                System.out.println("Fails later?");
                responseBuffer.write(input_topics_array_size);
            // Topics Array
                byte[][] input_topics_names = new byte[(input_topics_array_size[0])-1][];

                for(int i=0; i<input_topics_names.length; i++) {
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
        byte[] topic_name = new byte[(topic_name_length[0])-1];
        topic.read(topic_name);
        // Tag Buffer
        topic.read();
        return topic_name;
    }

    private byte[] generateTopicsArrayResponse(byte[][] input_topics_names) throws IOException{
        ByteArrayOutputStream topicsArrayBuffer = new ByteArrayOutputStream();
        byte tag_buffer = 0;
        MetadataBatches metadataBatches = clusterMetadataReader.parseClusterMetadataFile();
        for(int i=0; i<input_topics_names.length; i++) {
             // Find the Topic ID before writting
            byte[] topicId = new byte[16];
            topicId = metadataBatches.findTopicId(input_topics_names[i]);

            if (topicId == null) {
                // Error Code
                topicsArrayBuffer.write(shortToBytes(ErrorCodes.UNKOWN_TOPIC_ERROR_CODE));
            }
            else {
                topicsArrayBuffer.write(shortToBytes((short) 0));
            }
            // Topic name
            topicsArrayBuffer.write(input_topics_names[i].length+1);
            topicsArrayBuffer.write(input_topics_names[i]);
            // Topic ID
            if (topicId == null) {
                byte[] topicIdEmpty= new byte[16];
                Arrays.fill(topicIdEmpty, (byte) 0);
                topicsArrayBuffer.write(topicIdEmpty);
            }
            else {
                topicsArrayBuffer.write(topicId);
            }
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
