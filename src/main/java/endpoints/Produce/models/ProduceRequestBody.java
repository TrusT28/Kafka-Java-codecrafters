package endpoints.Produce.models;

import utils.ConstructorException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;

import static utils.NumbersUtils.readUnsignedVarInt;

public class ProduceRequestBody {
    public byte[] transactionId = new byte[1];
    public byte[] requiredACK = new byte[2]; //integer representing the number of acknowledgments. -1 means all-sync replicas must ack
    public byte[] timeout = new byte[4]; //time to await a response in milliseconds.
    public int topicsArrayLength; //The length of the topics array + 1, encoded as an unsigned varint
    public ArrayList<ProduceRequestTopic> topics = new ArrayList<>();

    public ProduceRequestBody(ByteArrayInputStream bodyStream) throws ConstructorException {
        try {
            System.out.println("Reading ProduceRequestBody");
            bodyStream.read(transactionId);
            bodyStream.read(requiredACK);
            bodyStream.read(timeout);
            topicsArrayLength = readUnsignedVarInt(bodyStream);
            for (int i = 0; i < topicsArrayLength-1; i++) {
                ProduceRequestTopic topic = new ProduceRequestTopic(bodyStream);
                topics.add(topic);
            }
            // TAG BUFFER
            bodyStream.read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
