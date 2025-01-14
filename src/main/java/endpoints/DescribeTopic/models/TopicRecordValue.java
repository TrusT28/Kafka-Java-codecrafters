package endpoints.DescribeTopic.models;

public class TopicRecordValue extends Value {
    public byte[] frameVersion = new byte[1];
    public byte[] type = new byte[1];
    public byte[] version = new byte[1];
    // unsigned varint
    public int nameLength;
    public byte[] topicName = null;
    public byte[] topicUUID = new byte[16];
    // unsigned varint
    public int taggedFieldsCount;
    public byte[] taggedFields = null;
}
