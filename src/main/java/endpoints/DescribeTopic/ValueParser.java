package endpoints.DescribeTopic;

import endpoints.DescribeTopic.models.*;

import java.io.IOException;
import java.io.InputStream;

public class ValueParser {
    public static Value readValue(InputStream inputStream, int valueLength) throws IOException {
        System.out.println("Reading value");
        byte[] frameVersion = new byte[1];
        inputStream.read(frameVersion);
        byte[] type = new byte[1];
        inputStream.read(type);
        byte[] version = new byte[1];
        inputStream.read(version);
        Value value = null;
        System.out.println("Record value type is "+ type[0]);
        switch (type[0]) {
            case 12:
                System.out.println("Value of type feature level");
                value = new FeatureLevelValue(inputStream);
                break;
            case 2:
                System.out.println("Value of type topic record");
                value = new TopicRecordValue(inputStream);
                break;
            case 3:
                System.out.println("Value of type partition record");
                value = new PartitionRecordValue(inputStream);
                break;
            default:
                System.out.println("Unknown Value of type " + type[0]);
                System.out.println("Will read dummy data of length " + valueLength);
                DummyValue dummyValue = new DummyValue();
                dummyValue.data = new byte[valueLength];
                inputStream.read(dummyValue.data);
                value = dummyValue;
                break;
        }
        value.frameVersion = frameVersion;
        value.type = type;
        value.version = version;
        return value;
    }
}