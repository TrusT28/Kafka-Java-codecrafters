package endpoints.Fetch.models;

import static utils.Utils.readUnsignedVarInt;

import java.io.IOException;
import java.io.InputStream;

import utils.ConstructorException;

public class FetchRequestTopics {
    public byte[] topicUUID = new byte[16];
    public int paritionsArrayLength;
    public FetchRequestPartitions[] paritions;
    
    public FetchRequestTopics(InputStream bodyStream) throws ConstructorException {
        try {
            bodyStream.read(topicUUID);
            paritionsArrayLength = readUnsignedVarInt(bodyStream);
            paritions = new FetchRequestPartitions[paritionsArrayLength-1];
            for(int i=0; i<paritions.length; i++) {
                paritions[i] = new FetchRequestPartitions(bodyStream);
            }
            // TAG_BUFFER
            bodyStream.read();
        }
        catch(IOException e) {
            throw new ConstructorException(e.getMessage());
        }
    }
}