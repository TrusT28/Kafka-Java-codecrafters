package endpoints.DescribeTopic.models;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MetadataBatches {
    public List<Batch> batchesArray = new ArrayList<>();

    public byte[] findTopicId(byte[] topicName) {
        System.out.println("Searching topic ID for topic name " + new String(topicName));
        if(batchesArray.size() > 0) {
            List<Batch> filteredBatches = batchesArray.stream().filter(batch -> !batch.isEmptyRecords()).collect(Collectors.toList());
            System.out.println("filteredBatches size: " + filteredBatches.size());
            if (!filteredBatches.isEmpty()) {
                for(int i=0; i<filteredBatches.size(); i++) {
                    Record[] records = filteredBatches.get(i).records;
                    System.out.println("This batch has records: " + records.length);
                    for(int j=0; j<records.length; j++){
                        byte[] result = findTopicId(records[j].value, topicName);
                        if(result != null) {
                            return result;
                        }
                    }
                }
            }
        }
        return null;
    }

    private byte[] findTopicId(Value value, byte[] topicName) {
        if(value.getClass()==TopicRecordValue.class) {
            System.out.println("Value is of TopicRecord type. Checking topic name match");
            TopicRecordValue topicRecordValue = (TopicRecordValue) value;
            System.out.println("Its topic name is: " + new String(topicRecordValue.topicName));
            if (new String(topicRecordValue.topicName).equals(new String(topicName))) {
                System.out.println("Topic name matches!");
                return topicRecordValue.topicUUID;
            }
            else {
                System.out.println("Topic name does not match.");
                return null;
            }
        }
        else return null;
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
