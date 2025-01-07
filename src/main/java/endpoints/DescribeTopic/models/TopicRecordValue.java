package endpoints.DescribeTopic.models;

public class TopicRecordValue extends Value {
    public byte[] frameVersion = new byte[1];
    public byte[] type = new byte[1];
    public byte[] version = new byte[1];
    public byte[] nameLength = new byte[1];
    public byte[] topicName = null;
    public byte[] topicUUID = new byte[16];
    public byte[] taggedFieldsCount = new byte[1];
    public byte[] taggedFields = null;
}
