package endpoints.Fetch.models;

import java.io.IOException;
import java.io.InputStream;

import utils.ConstructorException;

public class FetchRequestPartitions {
    public byte[] parition = new byte[4];
    public byte[] currentLeaderEpoch = new byte[4];
    public byte[] fetchOffset = new byte[8];
    public byte[] lastFetchedEpoch = new byte[4];
    public byte[] logStartOffset = new byte[8];
    public byte[] partitionMaxBytes = new byte[4]; 

    public FetchRequestPartitions(InputStream bodyStream) throws ConstructorException {
        try {
            bodyStream.read(parition);
            bodyStream.read(currentLeaderEpoch);
            bodyStream.read(fetchOffset);
            bodyStream.read(lastFetchedEpoch);
            bodyStream.read(logStartOffset);
            bodyStream.read(partitionMaxBytes);
            // TAG BUFFER
            bodyStream.read();
        }
        catch(IOException e) {
            throw new ConstructorException(e.getMessage());
        }
    }
}