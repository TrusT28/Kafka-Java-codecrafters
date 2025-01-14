package endpoints.DescribeTopic.models;

public class FeatureLevelValue extends Value {
    public byte[] frameVersion = new byte[1];
    public byte[] type = new byte[1];
    public byte[] version = new byte[1];
    // unsigned varint
    public int nameLength;
    public byte[] name = null;
    public byte[] featureLevel = new byte[2];
    // unsigned varint
    public int taggedFieldsCount;
    public byte[] taggedFields = null;
}
