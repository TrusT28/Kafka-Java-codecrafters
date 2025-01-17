package endpoints.DescribeTopic;

import static utils.Utils.bytesToInt;
import static utils.Utils.encodeVarInt;
import static utils.Utils.intToBytes;
import static utils.Utils.readUnsignedVarInt;
import static utils.Utils.shortToBytes;

import utils.ClusterMetadataException;
import utils.ErrorCodes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

import endpoints.KafkaEndpoint;
import endpoints.DescribeTopic.models.MetadataBatches;
import endpoints.DescribeTopic.models.PartitionRecordValue;
import api.RequestBody;

public class DescribeTopicEndpoint implements KafkaEndpoint {

    ClusterMetadataReader clusterMetadataReader = new ClusterMetadataReader();

    @Override
    public void process(RequestBody requestBody, ByteArrayOutputStream responseBuffer) throws IOException {
        // Only support 0-0 versions
        if (bytesToInt(requestBody.input_request_api_version) >= 0 && bytesToInt(requestBody.input_request_api_version) <= 0) {
            try {
                System.out.println("Handling a proper request");
                // Response Header
                    byte tag_buffer = 0;
                    responseBuffer.write(requestBody.input_correlation_id);
                    responseBuffer.write(tag_buffer);
                // Throttle time
                    byte[] throttle_time_ms = intToBytes(0);
                    responseBuffer.write(throttle_time_ms);
                // Topics Array
                    ByteArrayInputStream bodyStream = new ByteArrayInputStream(requestBody.body);
                    // Length of array
                    int input_topics_array_size = readUnsignedVarInt(bodyStream);
                    responseBuffer.write(encodeVarInt(input_topics_array_size));
                    System.out.println("Input topics size " + input_topics_array_size);
                    // Topics Array
                    byte[][] input_topics_names = new byte[input_topics_array_size-1][];

                    for(int i=0; i<input_topics_names.length; i++) {
                        input_topics_names[i] = readTopicName(bodyStream);
                    }

                    byte[] input_response_partition_limit = new byte[4];
                    bodyStream.read(input_response_partition_limit);
                    byte[] input_pagination_tag = new byte[1];
                    bodyStream.read(input_pagination_tag);
                    // Tag Buffer
                    bodyStream.read();

                    // Write topics array
                    System.out.println("Reading metadata kafka file");
                    MetadataBatches metadataBatches;
                    try {
                        metadataBatches = clusterMetadataReader.parseClusterMetadataFile();
                    }
                    catch(IOException e) {
                        System.out.println("Failed reading clusterMetadata file. " + e.getMessage());
                        throw e;
                    }
            
                    System.out.println("Done reading metadata kafka file. batches:"+ metadataBatches.batchesArray.size());
                    System.out.println("Total size of input topics names is " + input_topics_names.length);
                    Arrays.stream(input_topics_names).forEach(name -> System.out.println(new String(name)));
                    Map<String,byte[]> topicNameIdMap = metadataBatches.findTopicId(input_topics_names);
                    if (topicNameIdMap == null) {
                        System.out.println("topic names-ids map is null");
                        throw new ClusterMetadataException("topic names-ids map is null");
                    }

                    for(byte[] topicName: input_topics_names) {
                        byte[] topicId = topicNameIdMap.get(new String(topicName));
                        byte[] topicsArray = generateTopicResponse(topicName, topicId, bytesToInt(input_response_partition_limit), metadataBatches);
                        responseBuffer.write(topicsArray);
                        System.out.println("Topics Array for name " + new String(topicName) + " is done. Size " + topicsArray.length);
                    }
                // Next Cursor
                    responseBuffer.write(255);
                // Tag Buffer
                    responseBuffer.write(tag_buffer);
            } catch(ClusterMetadataException e) {
                System.out.println("Endpoint failed reading cluster metadata " + e.getMessage());
                return;
            }
            catch(IOException e) {
                System.out.println("Endpoint failed due to IO exception " + e.getMessage());
                return;
            }
        } else {
            // Throw appropriate error code
            System.out.println("Handling a wrong request");
            byte[] errorCode = shortToBytes((short) ErrorCodes.WRONG_REQUEST_ERROR_CODE);
            // specifies the size of the header and body.
            responseBuffer.write(requestBody.input_correlation_id);
            responseBuffer.write(errorCode);
        }
    }

    private byte[] readTopicName(ByteArrayInputStream topic) throws IOException{
        int topic_name_length = readUnsignedVarInt(topic);
        if(topic_name_length > 1) {
            byte[] topic_name = new byte[topic_name_length-1];
            topic.read(topic_name);
            // Tag Buffer
            topic.read();
            return topic_name;
        }
        else return null;
    }


    private byte[] generatePartitionsArray(byte[] topicId, int partitionsLimit, MetadataBatches metadataBatches) throws IOException {
        ByteArrayOutputStream partitionsArrayBuffer = new ByteArrayOutputStream();
        byte tagBuffer = 0;
        ArrayList<PartitionRecordValue> partitions = metadataBatches.findPartitions(topicId);
        if(partitions == null) {
            System.out.println("No partitions found");
            partitionsArrayBuffer.write(1);
        }
        else {
            System.out.println("Found partitions " + partitions.size());
            partitions.sort(Comparator.comparing(p -> bytesToInt(p.partitionId)));
            partitions.forEach(p -> System.out.println(bytesToInt(p.partitionId)));

            if(partitions.size() > partitionsLimit) {
                partitions = new ArrayList<PartitionRecordValue> (partitions.subList(0, partitionsLimit-1));
                System.out.println("Limiting partitions amount from limit " + partitionsLimit + " now size is " + partitions.size());
            }
            // Array length (+1 size)
            partitionsArrayBuffer.write(encodeVarInt(partitions.size()+1));
            for(PartitionRecordValue partition: partitions) {
                    // Error code
                    partitionsArrayBuffer.write(shortToBytes(ErrorCodes.NO_ERROR));
                    // Partition Id
                    partitionsArrayBuffer.write(partition.partitionId);
                    // Leader Id
                    partitionsArrayBuffer.write(partition.leader);
                    // Leader Epoch
                    partitionsArrayBuffer.write(partition.leaderEpoch);
                    // Replica Nodes
                        // array length
                        System.out.println("Replica nodes count: " + partition.replicaArrayLength);
                        partitionsArrayBuffer.write(encodeVarInt(partition.replicaArrayLength));
                        // replica ids
                        Optional.ofNullable(partition.replicaArray)
                            .ifPresent(array -> Arrays.stream(array).forEach(id -> {
                            try {
                                partitionsArrayBuffer.write(id);
                            } catch (IOException e) {
                                System.out.println("failed to write replica array");
                                e.printStackTrace();
                            }
                        }));
                    // ISR Nodes
                        // array length
                        partitionsArrayBuffer.write(encodeVarInt(partition.insyncReplicaArrayLength));
                        // replica ids
                        Optional.ofNullable(partition.insyncReplicaArray)
                            .ifPresent(array -> Arrays.stream(array).forEach(id -> {
                            try {
                                partitionsArrayBuffer.write(id);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }));
                    // Eligible Leader Replicas
                    partitionsArrayBuffer.write(1);
                    // Last Known ELR
                    partitionsArrayBuffer.write(1);
                    // Offline Replicas
                    partitionsArrayBuffer.write(1);
                    // Tag Buffer
                    partitionsArrayBuffer.write(tagBuffer);
            };
        }

        return partitionsArrayBuffer.toByteArray();
    }

    private byte[] generateTopicResponse(byte[] topicName, byte[] topicId, int partitionsLimit, MetadataBatches metadataBatches) throws IOException, ClusterMetadataException{
        ByteArrayOutputStream topicsArrayBuffer = new ByteArrayOutputStream();
        byte tag_buffer = 0;
        System.out.println("Writting response for topic name: " + new String(topicName));

        if(topicId == null) {
            System.out.println("topicID is null");

            // Error Code
            topicsArrayBuffer.write(shortToBytes(ErrorCodes.UNKOWN_TOPIC_ERROR_CODE));
            // Topic Name
                // String Length
                int nameLength = topicName.length+1;
                byte[] nameLengthEncoded = encodeVarInt(nameLength);
                topicsArrayBuffer.write(nameLengthEncoded);
                // String Content
                topicsArrayBuffer.write(topicName);
            // Topic ID
            byte[] topicIdEmpty= new byte[16];
            Arrays.fill(topicIdEmpty, (byte) 0);
            topicsArrayBuffer.write(topicIdEmpty);
            // Is Internal
            topicsArrayBuffer.write(0);
            // Partitions Array
            topicsArrayBuffer.write(1);
        }
        else {
            System.out.println("topicID exists");
            // Error Code
            topicsArrayBuffer.write(shortToBytes(ErrorCodes.NO_ERROR));
            // Topic Name
                // String Length
                int nameLength = topicName.length+1;
                byte[] nameLengthEncoded = encodeVarInt(nameLength);
                topicsArrayBuffer.write(nameLengthEncoded);
                // String Content
                topicsArrayBuffer.write(topicName);
            // Topic ID
            topicsArrayBuffer.write(topicId);
            // Is Internal
            topicsArrayBuffer.write(0);
            // Partitions Array
            byte[] parittionsArray = generatePartitionsArray(topicId, partitionsLimit, metadataBatches);
            topicsArrayBuffer.write(parittionsArray);
        }

        // Topic Authorized Operations
        byte[] authorizedOperations = {0,0,0,0,1,1,0,1,1,1,1,1,1,0,0,0};
        topicsArrayBuffer.write(authorizedOperations);
        // Tag Buffer
        topicsArrayBuffer.write(tag_buffer);
        return topicsArrayBuffer.toByteArray();
    }
}
