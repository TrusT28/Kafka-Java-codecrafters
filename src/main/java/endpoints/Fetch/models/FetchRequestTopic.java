package endpoints.Fetch.models;

import static utils.NumbersUtils.readUnsignedVarInt;

import java.io.IOException;
import java.io.InputStream;

import utils.ConstructorException;

public class FetchRequestTopic {
    public byte[] topicUUID = new byte[16];
    public int paritionsArrayLength;
    public FetchRequestPartitions[] paritions;
    
    public FetchRequestTopic(InputStream bodyStream) throws ConstructorException {
        try {
            System.out.println("Reading FetchRequestTopic");
            bodyStream.read(topicUUID);
            paritionsArrayLength = readUnsignedVarInt(bodyStream);
            System.out.println("paritionsArrayLength " + paritionsArrayLength);
            if (paritionsArrayLength > 1) {
                paritions = new FetchRequestPartitions[paritionsArrayLength-1];
                for(int i=0; i<paritions.length; i++) {
                    paritions[i] = new FetchRequestPartitions(bodyStream);
                }
            }
            // TAG_BUFFER
            bodyStream.read();
        }
        catch(IOException e) {
            throw new ConstructorException(e.getMessage());
        }
    }
}