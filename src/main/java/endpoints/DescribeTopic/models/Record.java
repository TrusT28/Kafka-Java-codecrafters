package endpoints.DescribeTopic.models;

import java.io.IOException;
import java.io.InputStream;

import static utils.NumbersUtils.readSignedVarInt;
import static utils.NumbersUtils.readUnsignedVarInt;

import static endpoints.DescribeTopic.ValueParser.readValue;

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

        public Record(InputStream inputStream) throws IOException {
                recordLength = readSignedVarInt(inputStream);
                System.out.println("This record length is " + recordLength);

                inputStream.read(attributes);
                timestampDelta = readSignedVarInt(inputStream);
                offsetDelta = readSignedVarInt(inputStream);

                keyLength = readSignedVarInt(inputStream);
                System.out.println("This keyLength is " + keyLength);
                if(keyLength > 0){ // Fixed: was != -1 || > 0, should be just > 0
                        key = new byte[keyLength]; // Fixed: removed duplicate declaration
                        inputStream.read(key);
                }

                valueLength = readSignedVarInt(inputStream);
                System.out.println("This valueLength is " + valueLength);
                if(valueLength != -1){
                        value = readValue(inputStream, valueLength);
                }

                headersArrayCount = readUnsignedVarInt(inputStream);
                System.out.println("This headersArrayCount is " + headersArrayCount);
                if(headersArrayCount > 0){
                        for(int i=0; i<headersArrayCount; i++) {
                                // TODO parse headers
                        }
                }
        }
}