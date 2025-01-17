package endpoints.DescribeTopic.models;

public class Record {
        public int recordLength;
        public byte[] attributes = new byte[1];
        // signed varint
        public int timestampDelta;
        // signed varint
        public int offsetDelta;
        public int keyLength;
        public byte[] key = null;
        public int valueLength;
        public Value value = null;
        public int headersArrayCount;
        public byte[] headersArray = null;
}