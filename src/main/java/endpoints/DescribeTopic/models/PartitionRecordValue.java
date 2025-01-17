package endpoints.DescribeTopic.models;

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
}
