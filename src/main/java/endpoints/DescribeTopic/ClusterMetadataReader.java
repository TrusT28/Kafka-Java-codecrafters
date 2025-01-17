package endpoints.DescribeTopic;

import static utils.Utils.bytesToInt;
import static utils.Utils.readSignedVarInt;
import static utils.Utils.readUnsignedVarInt;

import java.beans.FeatureDescriptor;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
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
            File file = new File(fileName);
            System.out.println("File exists? " + file.exists());
            System.out.println("File total length: " + file.length());
            ByteArrayInputStream inputStream = new ByteArrayInputStream(FileUtils.readFileToByteArray(file));
            MetadataBatches batches = new MetadataBatches();
            try {
                // Read Record Batches
                while(true) {
                    Batch batch = new Batch();
                    // TODO Fix this check
                    System.out.println("batch availability : " + inputStream.available());
                    if(inputStream.available()<=0) {
                        System.out.println("Closing loop");
                        inputStream.close();
                        break;
                    }
                    System.out.println("Continuing loop");
                    inputStream.read(batch.baseOffset);
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
                    System.out.println("Read amount of records:" + bytesToInt(batch.amountOfRecords));
                    if(bytesToInt(batch.amountOfRecords)>0) {
                        System.out.println("There are " + bytesToInt(batch.amountOfRecords) + " records");
                        Record[] records = new Record[bytesToInt(batch.amountOfRecords)];
                        for(int i=0; i<records.length; i++) {
                            records[i] = readRecord(inputStream);
                        }
                        batch.records = records;
                    }
                    batches.batchesArray.add(batch);
                    System.out.println("Added batch, now we have " + batches.batchesArray.size());
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

            record.recordLength = readSignedVarInt(inputStream);
            System.out.println("This record length is " + record.recordLength);

            inputStream.read(record.attributes);
            record.timestampDelta = readSignedVarInt(inputStream);
            record.offsetDelta = readSignedVarInt(inputStream);

            record.keyLength = readSignedVarInt(inputStream);
            System.out.println("This keyLength is " + record.keyLength);
            if(record.keyLength != -1 || record.keyLength > 0){
                byte[] key = new byte[record.keyLength];
                inputStream.read(key);
                record.key = key;
            }
            record.valueLength = readSignedVarInt(inputStream);
            System.out.println("This valueLength is " + record.valueLength);
            if(record.valueLength != -1 || record.valueLength > 0 ){
                record.value = readValue(inputStream, record.valueLength);
            }

            record.headersArrayCount = readUnsignedVarInt(inputStream);
            System.out.println("This headersArrayCount is " + record.headersArrayCount);
            if(record.headersArrayCount > 0){
                for(int i=0; i<record.headersArrayCount; i++) {
                    // TODO parse headers
                }
            }
            return record;
        }

        public Value readValue(InputStream inputStream, int valueLength) throws IOException {
            System.out.println("Reading value");
            byte[] frameVersion = new byte[1];
            inputStream.read(frameVersion);
            byte[] type = new byte[1];
            inputStream.read(type);
            byte[] version = new byte[1];
            inputStream.read(version);
            Value value = null;
            System.out.println("Record value type is "+ type[0]);
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
            System.out.println("Reading Partition Record Value");
            PartitionRecordValue partitionRecordValue = new PartitionRecordValue();
            inputStream.read(partitionRecordValue.partitionId);
            inputStream.read(partitionRecordValue.topicUUID);
            System.out.println("Found PartitionId,TopicId: " + bytesToInt(partitionRecordValue.partitionId) + "," + Arrays.toString(partitionRecordValue.topicUUID));
            partitionRecordValue.replicaArrayLength = readUnsignedVarInt(inputStream);
            System.out.println("replicaArrayLength " + partitionRecordValue.replicaArrayLength);
            if(partitionRecordValue.replicaArrayLength > 1){
                byte[][] replicaArray = new byte[partitionRecordValue.replicaArrayLength-1][4];
                for(int i=0; i<replicaArray.length; i++) {
                    inputStream.read(replicaArray[i]);
                }
                partitionRecordValue.replicaArray = replicaArray;
                System.out.println("replicaArray size " + partitionRecordValue.replicaArray.length);
            }
            
            partitionRecordValue.insyncReplicaArrayLength = readUnsignedVarInt(inputStream);
            System.out.println("insyncReplicaArrayLength size " + partitionRecordValue.insyncReplicaArrayLength);
            if(partitionRecordValue.insyncReplicaArrayLength > 1){
                byte[][] insyncReplicaArray = new byte[partitionRecordValue.insyncReplicaArrayLength-1][4];
                for(int i=0; i<insyncReplicaArray.length; i++) {
                    inputStream.read(insyncReplicaArray[i]);
                }
                partitionRecordValue.insyncReplicaArray = insyncReplicaArray;
            }

            partitionRecordValue.removingReplicaArrayLength = readUnsignedVarInt(inputStream);
            System.out.println("removingReplicaArrayLength size " + partitionRecordValue.removingReplicaArrayLength);
            if(partitionRecordValue.removingReplicaArrayLength>1){
                byte[][] removingReplicaArray = new byte[partitionRecordValue.removingReplicaArrayLength-1][4];
                for(int i=0; i<removingReplicaArray.length; i++) {
                    inputStream.read(removingReplicaArray[i]);
                }
                partitionRecordValue.removingReplicaArray = removingReplicaArray;
            }

            partitionRecordValue.addingReplicaArrayLength = readUnsignedVarInt(inputStream);
            System.out.println("addingReplicaArrayLength size " + partitionRecordValue.addingReplicaArrayLength);
            if(partitionRecordValue.addingReplicaArrayLength > 1){
                byte[][] addingReplicaArray = new byte[partitionRecordValue.addingReplicaArrayLength-1][4];
                for(int i=0; i<addingReplicaArray.length; i++) {
                    inputStream.read(addingReplicaArray[i]);
                }
                partitionRecordValue.addingReplicaArray = addingReplicaArray;
            }

            inputStream.read(partitionRecordValue.leader);
            inputStream.read(partitionRecordValue.leaderEpoch);
            inputStream.read(partitionRecordValue.partitionEpoch);
           
            partitionRecordValue.directoriesArrayLength = readUnsignedVarInt(inputStream);
            System.out.println("directoriesArrayLength size " + partitionRecordValue.directoriesArrayLength);
            if(partitionRecordValue.directoriesArrayLength > 1){
                byte[][] directoriesArray = new byte[partitionRecordValue.directoriesArrayLength-1][16];
                for(int i=0; i<directoriesArray.length; i++) {
                    inputStream.read(directoriesArray[i]);
                }
                partitionRecordValue.directoriesArray = directoriesArray;
            }

            partitionRecordValue.taggedFieldsCount = readUnsignedVarInt(inputStream);
            if(partitionRecordValue.taggedFieldsCount != 0){
                // TODO parse taggedFields
               byte[] taggedFields = new byte[partitionRecordValue.taggedFieldsCount];
               inputStream.read(taggedFields);
               partitionRecordValue.taggedFields = taggedFields;
           }

           return partitionRecordValue;
        }

        private TopicRecordValue readTopicRecordValue(InputStream inputStream) throws IOException {
            System.out.println("Reading topic record value");
            TopicRecordValue topicRecordValue = new TopicRecordValue();
            topicRecordValue.nameLength = readUnsignedVarInt(inputStream);
            if(topicRecordValue.nameLength != 0){
                byte[] name = new byte[topicRecordValue.nameLength-1];
                inputStream.read(name);
                topicRecordValue.topicName = name;
            }
            inputStream.read(topicRecordValue.topicUUID);
            System.out.println("Found Topic,Id: " + new String(topicRecordValue.topicName) + "," + Arrays.toString(topicRecordValue.topicUUID));
            topicRecordValue.taggedFieldsCount = readUnsignedVarInt(inputStream);
            if(topicRecordValue.taggedFieldsCount != 0){
                // TODO parse taggedFields
               byte[] taggedFields = new byte[topicRecordValue.taggedFieldsCount];
               inputStream.read(taggedFields);
               topicRecordValue.taggedFields = taggedFields;
           }
           return topicRecordValue;
        }

        private FeatureLevelValue readFeatureLevelValue(InputStream inputStream) throws IOException {
            System.out.println("Reading feature level value");
            FeatureLevelValue featureLevelValue = new FeatureLevelValue();
            featureLevelValue.nameLength = readUnsignedVarInt(inputStream);
            if(featureLevelValue.nameLength > 0){
                byte[] name = new byte[featureLevelValue.nameLength-1];
                inputStream.read(name);
                featureLevelValue.name = name;
            }
            inputStream.read(featureLevelValue.featureLevel);

            featureLevelValue.taggedFieldsCount = readUnsignedVarInt(inputStream);
            if(featureLevelValue.taggedFieldsCount > 0){
                 // TODO parse taggedFields
                byte[] taggedFields = new byte[featureLevelValue.taggedFieldsCount];
                inputStream.read(taggedFields);
                featureLevelValue.taggedFields = taggedFields;
            }
            return featureLevelValue;
        }
}
