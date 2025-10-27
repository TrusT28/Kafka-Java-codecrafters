package endpoints.Produce;

import api.RequestBody;
import endpoints.DescribeTopic.ClusterMetadataReader;
import endpoints.DescribeTopic.models.MetadataBatches;
import endpoints.DescribeTopic.models.Record;
import endpoints.DescribeTopic.models.TopicRecordValue;
import endpoints.Fetch.TopicMetadataReader;
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
        // Prepare topic Ids
        System.out.println("Reading metadata kafka file");
        MetadataBatches metadataBatches;
        ClusterMetadataReader clusterMetadataReader = new ClusterMetadataReader();
        try {
            metadataBatches = clusterMetadataReader.parseClusterMetadataFile();
        }
        catch(IOException e) {
            System.out.println("Failed reading clusterMetadata file. " + e.getMessage());
            throw e;
        }
        // Write response
        System.out.println("Writing Produce response");
        // Response Header
        responseBuffer.write(requestBody.input_correlation_id);
        responseBuffer.write(tag_buffer);
        // Response Body
        writeResponseBody(produceRequestBody, metadataBatches, responseBuffer);
    }

    private void writeResponseBody(ProduceRequestBody requestBody, MetadataBatches metadataBatches, ByteArrayOutputStream responseBuffer) throws IOException {
        byte tag_buffer = 0;
        // Get topic name -> UUID map (reverse of what Fetch uses)
        Map<String, byte[]> topicNameToUuidMap = metadataBatches.getTopicNameToUuidMap();

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
                    // Partition Index
                    responseBuffer.write(partition.partitionId);

                    // Validate topic and partition and write Error Code
                    short errorCode = validateTopicAndPartition(
                            new String(topic.topicName),
                            bytesToInt(partition.partitionId),
                            topicNameToUuidMap,
                            metadataBatches
                    );
                    responseBuffer.write(errorCode);
//                    // TODO This is hardcoded for now, we should detect real error
//                    TopicMetadataReader topicMetadataReader = new TopicMetadataReader();
//                    boolean topicExists = false;
//                    if (topic.topicName!=null && topic.topicName.length>0) {
//                        topicExists = topicMetadataReader.topicMetadataFileExists(Arrays.toString(topic.topicName));
//                        System.out.println("Topic exists: " + topicExists);
//                    }
//                    if(topicExists) {
//                        byte[] topicRecords = topicMetadataReader.readTopicRecords(Arrays.toString(topic.topicName));
//                        ByteArrayInputStream topicsAsStream = new ByteArrayInputStream(topicRecords);
//                        Record record = new Record(topicsAsStream);
//                        if(record.value.getClass() == TopicRecordValue.class) {
//                            TopicRecordValue value = (TopicRecordValue) record.value;
//                            if (value.topicName == topic.topicName) {
//                                responseBuffer.write(shortToBytes(ErrorCodes.NO_ERROR));
//                            }
//                            else{
//                                responseBuffer.write(shortToBytes(ErrorCodes.FETCH_UNKOWN_TOPIC_ERROR_CODE));
//                            }
//                        }
//                        else {
//                            responseBuffer.write(shortToBytes(ErrorCodes.FETCH_UNKOWN_TOPIC_ERROR_CODE));
//                        }
//                    }
//                    else {
//                        responseBuffer.write(shortToBytes(ErrorCodes.FETCH_UNKOWN_TOPIC_ERROR_CODE));
//                    }

//                    responseBuffer.write(shortToBytes((short) ErrorCodes.UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE));
                    // Base offset
                    byte[] baseOffset = longToBytes(-1);
                    responseBuffer.write(baseOffset);
                    // Log append time
                    byte[] logAppendTimeMs = longToBytes(-1);
                    responseBuffer.write(logAppendTimeMs);
                    // log_start_offset
                    byte[] logStartOffset = longToBytes(-1);
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

    private short validateTopicAndPartition(String topicName, int partitionIndex,
                                            Map<String, byte[]> topicNameToUuidMap,
                                            MetadataBatches metadataBatches) {
        // Step 1: Check if topic exists
        byte[] topicUuid = topicNameToUuidMap.get(topicName);
        if (topicUuid == null) {
            System.out.println("Topic does not exist: " + topicName);
            return ErrorCodes.UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE;
        }

        // Step 2: Check if partition exists for this topic
        boolean partitionExists = metadataBatches.partitionExists(topicUuid, partitionIndex);
        if (!partitionExists) {
            System.out.println("Partition " + partitionIndex + " does not exist for topic: " + topicName);
            return ErrorCodes.UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE;
        }

        System.out.println("Topic and partition validated successfully");
        return ErrorCodes.NO_ERROR;
    }
}
