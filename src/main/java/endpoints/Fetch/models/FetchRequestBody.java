package endpoints.Fetch.models;

import static utils.NumbersUtils.readUnsignedVarInt;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import utils.ConstructorException;

public class FetchRequestBody {
    public byte[] maxWaitMs = new byte[4];
    public byte[] minBytes = new byte[4];
    public byte[] maxBytes = new byte[4];
    public byte[] isolationLevel = new byte[1];
    public byte[] sessionId = new byte[4];
    public byte[] sessionEpoch = new byte[4];
    public int topicsArrayLength;
    public FetchRequestTopic[] topics;
    public int forgottenTopicsArrayLength;
    public FetchRequestForgottenTopics[] forgottenTopics;
    public int rackIdLength;
    public byte[] rackId;

    public FetchRequestBody(ByteArrayInputStream bodyStream) throws ConstructorException {
        try {
            System.out.println("Reading FetchRequestBody");
            bodyStream.read(maxWaitMs);
            bodyStream.read(minBytes);
            bodyStream.read(maxBytes);
            bodyStream.read(isolationLevel);
            bodyStream.read(sessionId);
            bodyStream.read(sessionEpoch);
            topicsArrayLength = readUnsignedVarInt(bodyStream);
            System.out.println("topicsArrayLength " + topicsArrayLength);
            if (topicsArrayLength > 1) {
                topics = new FetchRequestTopic[topicsArrayLength-1];
                for(int i=0; i<topics.length; i++) {
                    topics[i] = new FetchRequestTopic(bodyStream);
                }
            }
            forgottenTopicsArrayLength = readUnsignedVarInt(bodyStream);
            System.out.println("forgottenTopicsArrayLength " + forgottenTopicsArrayLength);
            if (forgottenTopicsArrayLength > 1) {
                forgottenTopics = new FetchRequestForgottenTopics[forgottenTopicsArrayLength-1];
                for(int i=0; i<forgottenTopics.length; i++) {
                    forgottenTopics[i] = new FetchRequestForgottenTopics(bodyStream);
                }
            }
            rackIdLength = readUnsignedVarInt(bodyStream);
            System.out.println("rackIdLength " + rackIdLength);
            if(rackIdLength > 1) {
                rackId = new byte[rackIdLength-1];
                bodyStream.read(rackId);
                System.out.println("rackId " + new String(rackId));
            }
            // TAG_BUFFER
            bodyStream.read();
        }
        catch(IOException e) {
            throw new ConstructorException(e.getMessage());
        }
    }
}
