package endpoints.DescribeTopic.models;

public class PartitionRecordValue extends Value {
    public byte[] frameVersion = new byte[1];
    public byte[] type = new byte[1];
    public byte[] version = new byte[1];
    public byte[] partitionId = new byte[4];
    public byte[] topicUUID = new byte[16];
    public byte[] replicaArrayLength = new byte[1];
    public byte[][] replicaArray = null; // array of 4 bytes replica ids
    public byte[] insyncReplicaArrayLength = new byte[1];
    public byte[][] insyncReplicaArray = null; // array of 4 bytes in-sync replica ids
    public byte[] removingReplicaArrayLength = new byte[1];
    public byte[][] removingReplicaArray = null; // array of 4 bytes removing replica ids
    public byte[] addingReplicaArrayLength = new byte[1];
    public byte[][] addingReplicaArray = null; // array of 4 bytes adding replica ids
    public byte[] leader = new byte[4];
    public byte[] leaderEpoch = new byte[4];
    public byte[] partitionEpoch = new byte[4];
    public byte[] directoriesArrayLength = new byte[1];
    public byte[][] directoriesArray = null; // array of 16 bytes uuids
    public byte[] taggedFieldsCount = new byte[1];
    public byte[] taggedFields = null; // TODO
}
