package endpoints.DescribeTopic;

import static utils.Utils.bytesToInt;
import static utils.Utils.encodeVarInt;
import static utils.Utils.encodeVarIntSigned;
import static utils.Utils.intToBytes;
import static utils.Utils.readUnsignedVarInt;
import static utils.Utils.shortToBytes;
import utils.ErrorCodes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import endpoints.KafkaEndpoint;
import endpoints.DescribeTopic.models.MetadataBatches;
import endpoints.DescribeTopic.models.PartitionRecordValue;
import api.RequestBody;

public class DescribeTopicEndpoint implements KafkaEndpoint {

    ClusterMetadataReader clusterMetadataReader = new ClusterMetadataReader();

    @Override
    public void process(RequestBody requestBody, OutputStream outputStream) throws IOException {
        System.out.println("outputStream inside describeTopicEndpoint is " + outputStream.hashCode());
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
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
        System.out.println("Finishing. Writting to output stream for describe Topic endpoint");
        outputStream.write(intToBytes(responseBytes.length));
        outputStream.write(responseBytes);
        outputStream.flush();
    }

    private byte[] readTopicName(ByteArrayInputStream topic) throws IOException{
        int topic_name_length = readUnsignedVarInt(topic);
        byte[] topic_name = new byte[topic_name_length-1];
        topic.read(topic_name);
        // Tag Buffer
        topic.read();
        return topic_name;
    }

    private byte[] generateTopicsArrayResponse(byte[][] input_topics_names) throws IOException{
        System.out.println("Reading metadata kafka file");
        ByteArrayOutputStream topicsArrayBuffer = new ByteArrayOutputStream();
        byte tag_buffer = 0;
        MetadataBatches metadataBatches = clusterMetadataReader.parseClusterMetadataFile();
        System.out.println("Done reading metadata kafka file. batches:"+metadataBatches.batchesArray.size());
        System.out.println("Total size of input topics names is " + input_topics_names.length);
        Arrays.stream(input_topics_names).forEach(name -> System.out.println(new String(name)));
        for(int i=0; i<input_topics_names.length; i++) {
             // Find the Topic ID before writting
            byte[] topicId = new byte[16];
            System.out.println("Writting response for topic name: " + new String(input_topics_names[i]));

            topicId = metadataBatches.findTopicId(input_topics_names[i]);
            System.out.println("its topic id is: " + topicId);
            if (topicId == null) {
                // Error Code
                topicsArrayBuffer.write(shortToBytes(ErrorCodes.UNKOWN_TOPIC_ERROR_CODE));
            }
            else {
                topicsArrayBuffer.write(shortToBytes((short) 0));
            }
            // Topic name
            int nameLength = input_topics_names[i].length+1;
            topicsArrayBuffer.write(encodeVarInt(nameLength));
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
            if (topicId == null) {
                topicsArrayBuffer.write(1);
            }
            else {
                ArrayList<PartitionRecordValue> partitions = metadataBatches.findPartitions(topicId);
                if(partitions!= null && partitions.size()>0) {
                    // TODO
                    partitions.sort(Comparator.comparing(p -> bytesToInt(p.partitionId)));
                    // Array length (+1 size)
                    topicsArrayBuffer.write(encodeVarInt(partitions.size()+1));
                    partitions.forEach(partition -> {
                        try {
                            // Error code
                            topicsArrayBuffer.write(shortToBytes(ErrorCodes.NO_ERROR));
                            // Partition Id
                            topicsArrayBuffer.write(partition.partitionId);
                            // Leader Id
                            topicsArrayBuffer.write(partition.leader);
                            // Leader Epoch
                            topicsArrayBuffer.write(partition.leaderEpoch);
                            // Replica Nodes
                                // array length
                                topicsArrayBuffer.write(encodeVarInt(partition.replicaArrayLength));
                                // replica ids
                                Arrays.stream(partition.replicaArray).forEach(id -> {
                                    try {
                                        topicsArrayBuffer.write(id);
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                });
                            // ISR Nodes
                                // array length
                                topicsArrayBuffer.write(encodeVarInt(partition.insyncReplicaArrayLength));
                                // replica ids
                                Arrays.stream(partition.insyncReplicaArray).forEach(id -> {
                                    try {
                                        topicsArrayBuffer.write(id);
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                });
                            //TODO how to get these?
                            // Eligible Leader Replicas
                            topicsArrayBuffer.write(1);
                            // Last Known ELR
                            topicsArrayBuffer.write(1);
                            // Offline Replicas
                            topicsArrayBuffer.write(1);
                            // Tag Buffer
                            topicsArrayBuffer.write(tag_buffer);
                        } catch (IOException e) {
                            // Handle the exception, for example, logging it or rethrowing as a runtime exception
                            e.printStackTrace();
                        }
                    });
                }
            }
           
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
