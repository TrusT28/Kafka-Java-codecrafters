package endpoints.DescribeTopic.models;

import utils.ConstructorException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;

import static utils.NumbersUtils.bytesToInt;

public class Batch {
    public byte[] baseOffset = new byte[8];
    public byte[] batchLength = new byte[4];
    public byte[] partitionLeaderEpoch = new byte[4];
    public byte[] magicByte = new byte[1];
    public byte[] crc = new byte[4];
    public byte[] attributes = new byte[2];
    public byte[] lastOffsetData = new byte[4];
    public byte[] baseTimestamp = new byte[8];
    public byte[] maxTimestamp = new byte[8];
    public byte[] producerID = new byte[8];
    public byte[] producerEpoch = new byte[2];
    public byte[] baseSequence = new byte[4];
    public byte[] amountOfRecords = new byte[4];
    public ArrayList<Record> records = new ArrayList<>();

    public Batch(ByteArrayInputStream inputStream) throws ConstructorException {
        try {
            inputStream.read(baseOffset);
            inputStream.read(batchLength);
            inputStream.read(partitionLeaderEpoch);
            inputStream.read(magicByte);
            inputStream.read(crc);

            inputStream.read(attributes);
            inputStream.read(lastOffsetData);
            inputStream.read(baseTimestamp);
            inputStream.read(maxTimestamp);
            inputStream.read(producerID);
            inputStream.read(producerEpoch);
            inputStream.read(baseSequence);
            inputStream.read(amountOfRecords);
            if(bytesToInt(amountOfRecords)>0) {
                System.out.println("There are " + bytesToInt(amountOfRecords) + " records");
                for(int i=0; i<bytesToInt(amountOfRecords); i++) {
                    records.add(new Record(inputStream));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
