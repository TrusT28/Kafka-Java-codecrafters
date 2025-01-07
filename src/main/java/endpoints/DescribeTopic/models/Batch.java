package endpoints.DescribeTopic.models;

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
    public Record[] records = null;

    public boolean isEmptyRecords() {
        return this.records==null || this.records.length==0;
    }
}
