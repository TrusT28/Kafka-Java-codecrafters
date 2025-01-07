package endpoints.DescribeTopic;

import static utils.Utils.bytesToInt;

import java.beans.FeatureDescriptor;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.math.BigInteger;
import org.apache.commons.io.FileUtils;

import endpoints.DescribeTopic.models.Batch;
import endpoints.DescribeTopic.models.DummyValue;
import endpoints.DescribeTopic.models.FeatureLevelValue;
import endpoints.DescribeTopic.models.MetadataBatches;
import endpoints.DescribeTopic.models.PartitionRecordValue;
import endpoints.DescribeTopic.models.Record;
import endpoints.DescribeTopic.models.TopicRecordValue;
import endpoints.DescribeTopic.models.Value;

public class ClusterMetadataReader {
    
        public MetadataBatches parseClusterMetadataFile() throws IOException {
            System.out.println("Parsing cluster metadata file");
            String fileName = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
            ByteArrayInputStream inputStream = new ByteArrayInputStream(FileUtils.readFileToByteArray(new File(fileName)));
            MetadataBatches batches = new MetadataBatches();
            try {
                // Read Record Batches
                while(true) {
                    Batch batch = new Batch();
                    // TODO make a better check
                    if(inputStream.read(batch.baseOffset)==-1) {
                        inputStream.close();
                        break;
                    }
                    int bigInteger = new BigInteger(batch.baseOffset).intValue();
                    System.out.println("baseOffset is " + batch.baseOffset + " or " + bigInteger);

                    inputStream.read(batch.batchLength);
                    inputStream.read(batch.partitionLeaderEpoch);
                    inputStream.read(batch.magicByte);
                    inputStream.read(batch.crc);

                    inputStream.read(batch.attributes);
                    inputStream.read(batch.lastOffsetData);
                    inputStream.read(batch.baseTimestamp);
                    inputStream.read(batch.maxTimestamp);
                    inputStream.read(batch.producerID);
                    inputStream.read(batch.producerEpoch);
                    inputStream.read(batch.baseSequence);
                    inputStream.read(batch.amountOfRecords);
                    if(bytesToInt(batch.amountOfRecords)>0) {
                        System.out.println("There are " + bytesToInt(batch.amountOfRecords) + " records");
                        Record[] records = new Record[bytesToInt(batch.amountOfRecords)];
                        for(int i=0; i<records.length; i++) {
                            records[i] = readRecord(inputStream);
                        }
                        batch.records = records;
                    }
                    batches.batchesArray.add(batch);
                }
                return batches;
            }
            finally {
                if(inputStream != null) {
                    try {
                        inputStream.close();
                    }
                    catch(IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        public Record readRecord(InputStream inputStream) throws IOException{
            Record record = new Record();
            inputStream.read(record.recordLength);
            System.out.println("This record length is "+ record.recordLength[0]);
            inputStream.read(record.attributes);
            inputStream.read(record.timestampData);
            inputStream.read(record.offsetDelta);
            inputStream.read(record.keyLength);
            if(record.keyLength[0] != -1){
                byte[] key = new byte[record.keyLength[0]];
                inputStream.read(key);
                record.key = key;
            }
            inputStream.read(record.valueLength);
            System.out.println("This record value length is "+ record.valueLength[0]);
            if(record.valueLength[0] != 0){
                record.value = readValue(inputStream, record.valueLength[0]);
            }

            inputStream.read(record.headersArrayCount);
            if(record.headersArrayCount[0] != 0){
                for(int i=0; i<record.headersArrayCount[0]; i++) {
                    // TODO parse headers
                }
            }
            return record;
        }

        public Value readValue(InputStream inputStream, int valueLength) throws IOException {
            byte[] frameVersion = new byte[1];
            inputStream.read(frameVersion);
            byte[] type = new byte[1];
            inputStream.read(type);
            byte[] version = new byte[1];
            inputStream.read(version);
            Value value = null;
            switch (type[0]) {
                case 12:
                    System.out.println("Value of type feature level");
                    value = readFeatureLevelValue(inputStream); 
                    break;
                case 2:
                    System.out.println("Value of type topic record");
                    value = readTopicRecordValue(inputStream);
                    break;
                case 3:
                    System.out.println("Value of type partition record");
                    value = readPartitionRecordValue(inputStream);
                    break;
                default:
                    System.out.println("Unkown Value of type " + type[0]);
                    System.out.println("Will read dummy data of length " + valueLength);
                    DummyValue dummyValue = new DummyValue();
                    dummyValue.data = new byte[valueLength];
                    inputStream.read(dummyValue.data);
                    value = dummyValue;
                    break;
            }
            value.frameVersion = frameVersion;
            value.type = type;
            value.version = version;
            return value;
        }

        private PartitionRecordValue readPartitionRecordValue(InputStream inputStream) throws IOException {
            PartitionRecordValue partitionRecordValue = new PartitionRecordValue();
            inputStream.read(partitionRecordValue.partitionId);
            inputStream.read(partitionRecordValue.topicUUID);

            inputStream.read(partitionRecordValue.replicaArrayLength);
            if(partitionRecordValue.replicaArrayLength[0] != 0){
                byte[][] replicaArray = new byte[partitionRecordValue.replicaArrayLength[0]-1][4];
                for(int i=0; i<replicaArray.length; i++) {
                    inputStream.read(replicaArray[i]);
                }
                partitionRecordValue.replicaArray = replicaArray;
            }

            inputStream.read(partitionRecordValue.insyncReplicaArrayLength);
            if(partitionRecordValue.insyncReplicaArrayLength[0] != 0){
                byte[][] insyncReplicaArray = new byte[partitionRecordValue.insyncReplicaArrayLength[0]-1][4];
                for(int i=0; i<insyncReplicaArray.length; i++) {
                    inputStream.read(insyncReplicaArray[i]);
                }
                partitionRecordValue.insyncReplicaArray = insyncReplicaArray;
            }

            inputStream.read(partitionRecordValue.removingReplicaArrayLength);
            if(partitionRecordValue.removingReplicaArrayLength[0] != 0){
                //TODO make sure removing replica has 4 bytes
                byte[][] removingReplicaArray = new byte[partitionRecordValue.removingReplicaArrayLength[0]-1][4];
                for(int i=0; i<removingReplicaArray.length; i++) {
                    inputStream.read(removingReplicaArray[i]);
                }
                partitionRecordValue.removingReplicaArray = removingReplicaArray;
            }

            inputStream.read(partitionRecordValue.addingReplicaArrayLength);
            if(partitionRecordValue.addingReplicaArrayLength[0] != 0){
                //TODO make sure adding replica has 4 bytes
                byte[][] addingReplicaArray = new byte[partitionRecordValue.addingReplicaArrayLength[0]-1][4];
                for(int i=0; i<addingReplicaArray.length; i++) {
                    inputStream.read(addingReplicaArray[i]);
                }
                partitionRecordValue.addingReplicaArray = addingReplicaArray;
            }

            inputStream.read(partitionRecordValue.leader);
            inputStream.read(partitionRecordValue.leaderEpoch);
            inputStream.read(partitionRecordValue.partitionEpoch);
           
            inputStream.read(partitionRecordValue.directoriesArrayLength);
            if(partitionRecordValue.directoriesArrayLength[0] != 0){
                byte[][] directoriesArray = new byte[partitionRecordValue.directoriesArrayLength[0]-1][16];
                for(int i=0; i<directoriesArray.length; i++) {
                    inputStream.read(directoriesArray[i]);
                }
                partitionRecordValue.directoriesArray = directoriesArray;
            }

            inputStream.read(partitionRecordValue.taggedFieldsCount);
            if(partitionRecordValue.taggedFieldsCount[0] != 0){
                // TODO parse taggedFields
               byte[] taggedFields = new byte[partitionRecordValue.taggedFieldsCount[0]];
               inputStream.read(taggedFields);
               partitionRecordValue.taggedFields = taggedFields;
           }

           return partitionRecordValue;
        }

        private TopicRecordValue readTopicRecordValue(InputStream inputStream) throws IOException {
            TopicRecordValue topicRecordValue = new TopicRecordValue();
            inputStream.read(topicRecordValue.nameLength);
            if(topicRecordValue.nameLength[0] != 0){
                byte[] name = new byte[topicRecordValue.nameLength[0]-1];
                inputStream.read(name);
                topicRecordValue.topicName = name;
            }
            inputStream.read(topicRecordValue.topicUUID);
            inputStream.read(topicRecordValue.taggedFieldsCount);
            if(topicRecordValue.taggedFieldsCount[0] != 0){
                // TODO parse taggedFields
               byte[] taggedFields = new byte[topicRecordValue.taggedFieldsCount[0]];
               inputStream.read(taggedFields);
               topicRecordValue.taggedFields = taggedFields;
           }
           return topicRecordValue;
        }

        private FeatureLevelValue readFeatureLevelValue(InputStream inputStream) throws IOException {
            FeatureLevelValue featureLevelValue = new FeatureLevelValue();
            inputStream.read(featureLevelValue.nameLength);
            if(featureLevelValue.nameLength[0] != 0){
                byte[] name = new byte[featureLevelValue.nameLength[0]-1];
                inputStream.read(name);
                featureLevelValue.name = name;
            }
            inputStream.read(featureLevelValue.featureLevel);
            inputStream.read(featureLevelValue.taggedFieldsCount);
            if(featureLevelValue.taggedFieldsCount[0] != 0){
                 // TODO parse taggedFields
                byte[] taggedFields = new byte[featureLevelValue.taggedFieldsCount[0]];
                inputStream.read(taggedFields);
                featureLevelValue.taggedFields = taggedFields;
            }
            return featureLevelValue;
        }
}
