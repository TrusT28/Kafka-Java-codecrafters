package endpoints.DescribeTopic.models;

public class Record {
        public int recordLength;
        public byte[] attributes = new byte[1];
        // TODO varint
        public byte[] timestampData = new byte[1];
         // TODO varint
        public byte[] offsetDelta = new byte[1];
         // TODO varint
        public byte[] keyLength = new byte[1];
        public byte[] key = null;
        public byte[] valueLength = new byte[1];
        public Value value = null;
        public byte[] headersArrayCount = new byte[1];
        public byte[] headersArray = null;
}