package endpoints.DescribeTopic.models;

import java.util.ArrayList;
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
            if (new String(topicRecordValue.topicName) == new String(topicName)) {
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

}
