package endpoints.DescribeTopic.models;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static utils.NumbersUtils.bytesToInt;
import static utils.NumbersUtils.readUnsignedVarInt;

public class PartitionRecordValue extends Value {
    public byte[] frameVersion = new byte[1];
    public byte[] type = new byte[1];
    public byte[] version = new byte[1];
    public byte[] partitionId = new byte[4];
    public byte[] topicUUID = new byte[16];
    // unsigned varint
    public int replicaArrayLength;
    public byte[][] replicaArray = null; // array of 4 bytes replica ids
    // unsigned varint
    public int insyncReplicaArrayLength;
    public byte[][] insyncReplicaArray = null; // array of 4 bytes in-sync replica ids
    // unsigned varint
    public int removingReplicaArrayLength;
    public byte[][] removingReplicaArray = null; // array of 4 bytes removing replica ids
    // unsigned varint
    public int addingReplicaArrayLength;
    public byte[][] addingReplicaArray = null; // array of 4 bytes adding replica ids
    public byte[] leader = new byte[4];
    public byte[] leaderEpoch = new byte[4];
    public byte[] partitionEpoch = new byte[4];
    // unsigned varint
    public int directoriesArrayLength;
    public byte[][] directoriesArray = null; // array of 16 bytes uuids
    // unsigned varint
    public int taggedFieldsCount;
    public byte[] taggedFields = null;

    public PartitionRecordValue(InputStream inputStream) throws IOException {
        System.out.println("Reading Partition Record Value");
        inputStream.read(partitionId);
        inputStream.read(topicUUID);
        System.out.println("Found PartitionId,TopicId: " + bytesToInt(partitionId) + "," + Arrays.toString(topicUUID));
        replicaArrayLength = readUnsignedVarInt(inputStream);
        System.out.println("replicaArrayLength " + replicaArrayLength);
        if(replicaArrayLength > 1){
            replicaArray = new byte[replicaArrayLength-1][4];
            for (int i = 0; i < replicaArray.length; i++) {
                byte[] bytes = new byte[4];
                inputStream.read(bytes);
                replicaArray[i] = bytes;
            }
            System.out.println("replicaArray size " + replicaArray.length);
        }

        insyncReplicaArrayLength = readUnsignedVarInt(inputStream);
        System.out.println("insyncReplicaArrayLength size " + insyncReplicaArrayLength);
        if(insyncReplicaArrayLength > 1){
            insyncReplicaArray = new byte[insyncReplicaArrayLength-1][4];
            for (int i = 0; i < insyncReplicaArray.length; i++) {
                byte[] bytes = new byte[4];
                inputStream.read(bytes);
                insyncReplicaArray[i] = bytes;
            }
        }

        removingReplicaArrayLength = readUnsignedVarInt(inputStream);
        System.out.println("removingReplicaArrayLength size " + removingReplicaArrayLength);
        if(removingReplicaArrayLength>1){
            removingReplicaArray = new byte[removingReplicaArrayLength-1][4];
            for (int i = 0; i < removingReplicaArray.length; i++) {
                byte[] bytes = new byte[4];
                inputStream.read(bytes);
                removingReplicaArray[i] = bytes;
            }
        }

        addingReplicaArrayLength = readUnsignedVarInt(inputStream);
        System.out.println("addingReplicaArrayLength size " + addingReplicaArrayLength);
        if(addingReplicaArrayLength > 1){
            addingReplicaArray = new byte[addingReplicaArrayLength-1][4];
            for (int i = 0; i < addingReplicaArray.length; i++) {
                byte[] bytes = new byte[4];
                inputStream.read(bytes);
                removingReplicaArray[i] = bytes;
            }
        }

        inputStream.read(leader);
        inputStream.read(leaderEpoch);
        inputStream.read(partitionEpoch);

        directoriesArrayLength = readUnsignedVarInt(inputStream);
        System.out.println("directoriesArrayLength size " + directoriesArrayLength);
        if(directoriesArrayLength > 1){
            directoriesArray = new byte[directoriesArrayLength-1][16];
            for (int i = 0; i < directoriesArray.length; i++) {
                byte[] bytes = new byte[16];
                inputStream.read(bytes);
                directoriesArray[i] = bytes;
            }
        }

        taggedFieldsCount = readUnsignedVarInt(inputStream);
        if(taggedFieldsCount != 0){
            // TODO parse taggedFields
            taggedFields = new byte[taggedFieldsCount];
            inputStream.read(taggedFields);
        }
    }
}
