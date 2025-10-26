package endpoints.Produce.models;

import utils.ConstructorException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static utils.NumbersUtils.readUnsignedVarInt;

public class ProduceRequestTopic {
    public int topicNameLength; //The length of the string + 1, unsigned varint
    public byte[] topicName = null; // size is topicNameLength
    public int partitionsArrayLength; //The length of the partitions array + 1, unsigned varint
    public ArrayList<ProduceRequestPartition> partitionsArray = new ArrayList<>();

    public ProduceRequestTopic(ByteArrayInputStream bodyStream) throws ConstructorException {
        try {
            // Unify topic Name creation across endpoints
            topicNameLength = readUnsignedVarInt(bodyStream);
            if (topicNameLength > 1){
                byte[] name = new byte[topicNameLength-1];
                bodyStream.read(name);
                this.topicName = name;
                System.out.println("Topic Name is " + Arrays.toString(this.topicName));
            }
            partitionsArrayLength = readUnsignedVarInt(bodyStream);
            for (int i = 0; i < partitionsArrayLength-1; i++) {
                ProduceRequestPartition partition = new ProduceRequestPartition(bodyStream);
                partitionsArray.add(partition);
            }
            // TAG_BUFFER
            bodyStream.read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
