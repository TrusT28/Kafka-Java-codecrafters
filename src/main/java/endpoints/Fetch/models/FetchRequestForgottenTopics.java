package endpoints.Fetch.models;

import static utils.Utils.readUnsignedVarInt;

import java.io.IOException;
import java.io.InputStream;

import utils.ConstructorException;

public class FetchRequestForgottenTopics {
    public byte[] topicUUID = new byte[16];
    public int paritionsArrayLength;
    public byte[][] paritions;

    public FetchRequestForgottenTopics(InputStream bodyStream) throws ConstructorException {
        try {
            System.out.println("Reading FetchRequestBody");
            bodyStream.read(topicUUID);
            paritionsArrayLength = readUnsignedVarInt(bodyStream);
            paritions = new byte[paritionsArrayLength-1][4];
            for(int i=0; i<paritions.length; i++) {
                bodyStream.read(paritions[i]);
            }
            // TAG_BUFFER
            bodyStream.read();
        }
        catch(IOException e) {
            throw new ConstructorException(e.getMessage());
        }
    }
}
