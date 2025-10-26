package endpoints.DescribeTopic.models;

import java.io.IOException;
import java.io.InputStream;

import static utils.NumbersUtils.readUnsignedVarInt;

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

    public FeatureLevelValue(InputStream inputStream) throws IOException {
        System.out.println("Reading feature level value");
        nameLength = readUnsignedVarInt(inputStream);
        if(nameLength > 0){
            byte[] name = new byte[nameLength-1];
            inputStream.read(name);
        }
        inputStream.read(featureLevel);

        taggedFieldsCount = readUnsignedVarInt(inputStream);
        if(taggedFieldsCount > 0){
            // TODO parse taggedFields
            taggedFields = new byte[taggedFieldsCount];
            inputStream.read(taggedFields);
        }
    }
}
