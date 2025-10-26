package endpoints.DescribeTopic.models;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static utils.NumbersUtils.readUnsignedVarInt;

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

    public TopicRecordValue(InputStream inputStream) throws IOException {
        System.out.println("Reading topic record value");
        nameLength = readUnsignedVarInt(inputStream);
        if(nameLength != 0){
            topicName = new byte[nameLength-1];
            inputStream.read(topicName);
            System.out.println("topic Name is " + Arrays.toString(topicName));
        }
        inputStream.read(topicUUID);
        System.out.println("Found Topic,Id: " + new String(topicName) + "," + Arrays.toString(topicUUID));
        taggedFieldsCount = readUnsignedVarInt(inputStream);
        if(taggedFieldsCount != 0){
            // TODO parse taggedFields
            byte[] taggedFields = new byte[taggedFieldsCount];
            inputStream.read(taggedFields);
        }
    }
}
