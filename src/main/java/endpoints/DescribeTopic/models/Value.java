package endpoints.DescribeTopic.models;

public abstract class Value {
    public byte[] frameVersion = new byte[1];
    public byte[] type = new byte[1];
    public byte[] version = new byte[1];
}
