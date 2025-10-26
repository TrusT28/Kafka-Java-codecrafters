package endpoints.Produce.models;

import endpoints.DescribeTopic.models.Batch;
import utils.ConstructorException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;

import static utils.NumbersUtils.readUnsignedVarInt;

public class ProduceRequestPartition {
    public byte[] partitionId = new byte[4];
    public int recordBatchSize; //The length of the record batch + 1 encoded as an unsigned varint
    public ArrayList<Batch> batches;

    public ProduceRequestPartition(ByteArrayInputStream bodyStream) throws ConstructorException {
        try {
            bodyStream.read(partitionId);
            recordBatchSize = readUnsignedVarInt(bodyStream);
            for (int i = 0; i < recordBatchSize-1; i++) {
                Batch batch = new Batch(bodyStream);
                batches.add(batch);
            }
            // TAG_BUFFER
            bodyStream.read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
