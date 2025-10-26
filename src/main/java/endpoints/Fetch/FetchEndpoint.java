package endpoints.Fetch;

import static utils.NumbersUtils.bytesToInt;
import static utils.NumbersUtils.encodeVarInt;
import static utils.NumbersUtils.intToBytes;
import static utils.NumbersUtils.shortToBytes;

import utils.ConstructorException;
import utils.ErrorCodes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import api.RequestBody;
import endpoints.KafkaEndpoint;
import endpoints.DescribeTopic.ClusterMetadataReader;
import endpoints.DescribeTopic.models.MetadataBatches;
import endpoints.Fetch.models.FetchRequestBody;
import endpoints.Fetch.models.FetchRequestTopic;

public class FetchEndpoint implements KafkaEndpoint{

    @Override
    public void process(RequestBody requestBody, ByteArrayOutputStream responseBuffer) throws IOException, ConstructorException {
            // Only support 0-16 versions
            if (bytesToInt(requestBody.input_request_api_version) >= 0 && bytesToInt(requestBody.input_request_api_version) <= 16) {
                System.out.println("Handling a proper request");
                byte tag_buffer = 0;
                ByteArrayInputStream bodyStream = new ByteArrayInputStream(requestBody.body);
                // Parse request body
                FetchRequestBody fetchRequestBody;
                try {
                    fetchRequestBody = new FetchRequestBody(bodyStream);
                }
                catch(ConstructorException e) {
                    System.out.println("Failed to read Fetch Request body");
                    return;
                }
                catch(Exception e) {
                    System.out.println("Failed to read Fetch Request body. Unexpected error " + e.getStackTrace());
                    return;
                }

                // Prepare topic Ids - names
                Map<ByteBuffer, String> topicIdNameMap = null;
                if(fetchRequestBody.topics != null && fetchRequestBody.topics.length>0) {
                    System.out.println("Reading metadata kafka file");
                    ClusterMetadataReader clusterMetadataReader = new ClusterMetadataReader();
                    MetadataBatches metadataBatches;
                    try {
                        metadataBatches = clusterMetadataReader.parseClusterMetadataFile();
                    } catch (IOException e) {
                        System.out.println("Failed reading clusterMetadata file. " + e.getMessage());
                        throw e;
                    }
                    System.out.println("Done reading metadata kafka file. batches:" + metadataBatches.batchesArray.size());
                    topicIdNameMap = metadataBatches.getTopicIdNameMap();
                    if (topicIdNameMap == null) {
                        System.out.println("topic names-ids map is null");
                    }
                }

                // Write response
                System.out.println("Writing Fetch response");
                // Response Header
                responseBuffer.write(requestBody.input_correlation_id);
                responseBuffer.write(tag_buffer);

                writeResponseBody(fetchRequestBody, topicIdNameMap, responseBuffer);
            } else {
                // Throw appropriate error code
                System.out.println("Handling a wrong request");
                byte[] errorCode = shortToBytes((short) ErrorCodes.WRONG_REQUEST_ERROR_CODE);
                // specifies the size of the header and body.
                responseBuffer.write(requestBody.input_correlation_id);
                responseBuffer.write(errorCode);
            }
    }

    private void writeResponseBody(FetchRequestBody requestBody, Map<ByteBuffer, String> topicIdNameMap, ByteArrayOutputStream responseBuffer) throws IOException {
        byte[] errorCode = shortToBytes((short) ErrorCodes.NO_ERROR);
        byte[] throttle_time_ms = intToBytes(0);
        byte tag_buffer = 0;

        responseBuffer.write(throttle_time_ms);
        responseBuffer.write(errorCode);
        // Session id
        responseBuffer.write(intToBytes(0));
        // Responses
        System.out.println("Response length is " + requestBody.topicsArrayLength);
        // Responses Length
        responseBuffer.write(encodeVarInt(requestBody.topicsArrayLength));
        if(requestBody.topicsArrayLength>1) {
            for(FetchRequestTopic topic : requestBody.topics) {
                System.out.println("Writing for topic id " + Arrays.toString(topic.topicUUID));
                // Topic ID
                responseBuffer.write(topic.topicUUID);
                // Partitions Array
                String topicName = topicIdNameMap.get(ByteBuffer.wrap(topic.topicUUID));
                if(topicName==null) {
                    System.out.println("topicName is null");
                }
                else {
                    System.out.println("Its topicName is " + topicName);
                }

                byte[] partitionsResponse = writeTopicPartitions(topic, topicName);
                responseBuffer.write(partitionsResponse);
            }
        }
        // Tag Buffer
        responseBuffer.write(tag_buffer);
        responseBuffer.write(tag_buffer);
    }

    private byte[] writeTopicPartitions(FetchRequestTopic topic, String topicName) throws IOException {
        ByteArrayOutputStream partitionsResponseBuffer = new ByteArrayOutputStream();
        TopicMetadataReader topicMetadataReader = new TopicMetadataReader();
        byte tagBuffer = 0;
        
        boolean topicExists;
        if (topicName!=null && !topicName.isEmpty()) {
            topicExists = topicMetadataReader.topicMetadataFileExists(topicName);
            System.out.println("Topic exists: " + topicExists);
        }
        else {
            topicExists = false;
        }

        // Length
        partitionsResponseBuffer.write(encodeVarInt(2));
        // Partition index
        partitionsResponseBuffer.write(intToBytes(0));
        // Error code
        
        if(topicExists) {
            partitionsResponseBuffer.write(shortToBytes(ErrorCodes.NO_ERROR));
        }
        else {
            partitionsResponseBuffer.write(shortToBytes(ErrorCodes.FETCH_UNKOWN_TOPIC_ERROR_CODE));
        }
        // high_watermark => INT64
        byte[] highWaterMark = new byte[8];
        partitionsResponseBuffer.write(highWaterMark);
        // last_stable_offset => INT64
        byte[] lastStableOffset = new byte[8];
        partitionsResponseBuffer.write(lastStableOffset);
        // log_start_offset => INT64
        byte[] logStartOffset = new byte[8];
        partitionsResponseBuffer.write(logStartOffset);
        // aborted_transactions => producer_id first_offset TAG_BUFFER 
        // Length
        partitionsResponseBuffer.write(encodeVarInt(0));
            // producer_id => INT64
            // first_offset => INT64
        // preferred_read_replica => INT32
        byte[] preferredReadReplica = new byte[4];
        partitionsResponseBuffer.write(preferredReadReplica);
        // records => COMPACT_RECORDS
        if(topicExists) {
            byte[] topicRecords = topicMetadataReader.readTopicRecords(topicName);
            partitionsResponseBuffer.write(encodeVarInt(topicRecords.length+1));
            partitionsResponseBuffer.write(topicRecords);
        }
        else {
            partitionsResponseBuffer.write(encodeVarInt(0));
        }
        partitionsResponseBuffer.write(tagBuffer);

        return partitionsResponseBuffer.toByteArray();
    }
}
