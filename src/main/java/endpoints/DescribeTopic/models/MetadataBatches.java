package endpoints.DescribeTopic.models;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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
                            System.out.println("This value does not match anything" + new String(topicRecordValue.topicName));
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
        System.out.println("Searching partitions for topic ID " + new String(topicId));
        if(batchesArray.size() > 0) {
            List<Batch> filteredBatches = batchesArray.stream().filter(batch -> !batch.isEmptyRecords()).collect(Collectors.toList());
            System.out.println("filteredBatches size: " + filteredBatches.size());
            if (!filteredBatches.isEmpty()) {
                ArrayList<PartitionRecordValue> partitions = new ArrayList<>();
                for(int i=0; i<filteredBatches.size(); i++) {
                    Record[] records = filteredBatches.get(i).records;
                    System.out.println("This batch has records: " + records.length);
                    for(int j=0; j<records.length; j++){
                        PartitionRecordValue result = findPartitions(records[j].value, topicId);
                        if(result != null) {
                            partitions.add(result);
                        }
                    }
                }
                if (partitions.size()>0) {
                    return partitions;
                }
            }
        }
        return null;
    }

    private PartitionRecordValue findPartitions(Value value, byte[] topicId) {
        if(value.getClass()==PartitionRecordValue.class) {
            System.out.println("Value is of PartitionRecordValue type. Checking topic id match");
            PartitionRecordValue partitionRecordValue = (PartitionRecordValue) value;
            System.out.println("Its topic id is: " + new String(partitionRecordValue.topicUUID));
            if (Arrays.equals(partitionRecordValue.topicUUID,topicId)) {
                System.out.println("Topic ids match!");
                return partitionRecordValue;
            }
            else {
                System.out.println("Topic id does not match.");
                return null;
            }
        }
        else return null;
    }

}
