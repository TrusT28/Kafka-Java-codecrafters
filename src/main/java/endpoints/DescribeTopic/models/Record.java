package endpoints.DescribeTopic.models;

public class Record {
        public int recordLength;
        public byte[] attributes = new byte[1];
        // TODO varint
        public byte[] timestampData = new byte[1];
         // TODO varint
        public byte[] offsetDelta = new byte[1];
        public int keyLength;
        public byte[] key = null;
        public int valueLength;
        public Value value = null;
        public int headersArrayCount;
        public byte[] headersArray = null;
}