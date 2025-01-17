package endpoints.DescribeTopic.models;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import utils.ClusterMetadataException;

public class MetadataBatches {
    public List<Batch> batchesArray = new ArrayList<>();

    public Map<String,byte[]> findTopicId(byte[][] topicNames) {
        if(batchesArray.size() > 0) {
            Map<String,byte[]> topicNameIdMap = new HashMap<>();
            for(Batch batch: batchesArray) {
                System.out.println("This batch has records: " + batch.records.length);
                for(Record record: batch.records){
                    if(topicNameIdMap.size() == topicNames.length) {
                        return topicNameIdMap;
                    }
                    if(record.value.getClass()==TopicRecordValue.class) {
                        TopicRecordValue topicRecordValue = (TopicRecordValue) record.value;
                        Optional<byte[]> match = Arrays.stream(topicNames).filter(name -> Arrays.equals(name, topicRecordValue.topicName)).findFirst();
                        if(match.isPresent()) {
                            topicNameIdMap.put(new String(topicRecordValue.topicName), topicRecordValue.topicUUID);
                            System.out.println("Found a match for " + new String(topicRecordValue.topicName));
                        }
                        else {
                            System.out.println("This value does not match anything " + new String(topicRecordValue.topicName));
                        }
                    }
                }
            }
            return topicNameIdMap;
        }
        return null;
    }

    
    // TODO maybe relation of records within a batch is important. If I found topic record value, I can find based on that record other data?
    public ArrayList<PartitionRecordValue> findPartitions(byte[] topicId) {
        if(batchesArray.size() > 0) {
                ArrayList<PartitionRecordValue> partitions = new ArrayList<>();
                for(Batch batch: batchesArray) {
                    for(Record record: batch.records){
                        if(record.value.getClass()==PartitionRecordValue.class) {
                            PartitionRecordValue partitionRecordValue = (PartitionRecordValue) record.value;
                            if (Arrays.equals(partitionRecordValue.topicUUID,topicId)) {
                                System.out.println("Topic ids match!");
                                partitions.add(partitionRecordValue);
                            }
                        }
                    }
                }
                if(partitions.size()>0)
                    return partitions;
                else {
                    System.out.println("Found no partitions");
                    return null;
                }
            }
        return null;
    }


        public Map<ByteBuffer, String> getTopicIdNameMap() throws IOException {
            if(batchesArray.size() > 0) {
                Map<ByteBuffer, String> topicIdNameMap = new HashMap<>();
                for(Batch batch: batchesArray) {
                    System.out.println("This batch has records: " + batch.records.length);
                    for(Record record: batch.records){
                        if(record.value.getClass()==TopicRecordValue.class) {
                            TopicRecordValue topicRecordValue = (TopicRecordValue) record.value;
                            System.out.println("Found topic " + new String(topicRecordValue.topicName));
                            topicIdNameMap.put(ByteBuffer.wrap(topicRecordValue.topicUUID), new String(topicRecordValue.topicName));
                        }
                    }
                }
                return topicIdNameMap;
            }
            return null;
        }

}
