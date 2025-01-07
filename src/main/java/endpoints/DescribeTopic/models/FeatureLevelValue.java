package endpoints.DescribeTopic.models;

public class FeatureLevelValue extends Value {
    public byte[] frameVersion = new byte[1];
    public byte[] type = new byte[1];
    public byte[] version = new byte[1];
    public byte[] nameLength = new byte[1];
    public byte[] name = null;
    public byte[] featureLevel = new byte[2];
    public byte[] taggedFieldsCount = new byte[1];
    public byte[] taggedFields = null;
}
