package utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Utils {
    public static int bytesToInt(byte[] bytes) {
        if (bytes.length == 4) {
            return ByteBuffer.wrap(bytes).getInt();
        }
        else if(bytes.length == 2) {
            return ByteBuffer.wrap(bytes).getShort();
        }
        else {
            throw new IllegalArgumentException("Array must contain exactly 2 or 4 bytes");
        }
  }

  public static byte[] shortToBytes(short value) {
    System.out.println("shortToBytes "+ value);
      return ByteBuffer.allocate(2).putShort(value).array();
  }

  public static byte[] intToBytes(int value) {
    System.out.println("intToBytes "+ value);
      return ByteBuffer.allocate(4).putInt(value).array();
  }

  
  // Helper method to read a variable-length integer.
  public static int readUnsignedVarInt(InputStream inputStream) throws IOException {
      int value = 0;
      int position = 0;

      while (true) {
          int currentByte = inputStream.read();
          if (currentByte == -1) {
              throw new IOException("Unexpected end of stream while reading varint.");
          }

          // Extract the lower 7 bits and shift them to the correct position.
          value |= (currentByte & 0x7F) << (position * 7);

          // If the MSB is not set, we've reached the end of the varint.
          if ((currentByte & 0x80) == 0) {
              break;
          }

          position++;

          // Prevent overflow for excessively large varints.
          if (position >= 5) { // Varints should not exceed 5 bytes (35 bits).
              throw new IOException("Varint is too large.");
          }
      }
      return value;
  }

  // Helper method to read a variable-length integer.
  public static int readSignedVarInt(InputStream inputStream) throws IOException {
      int value = readUnsignedVarInt(inputStream);
      // Apply ZigZag decoding to interpret signed integers.
      return (value >>> 1) ^ -(value & 1);
  }


  // Method to encode a signed int as a variable-length integer (VarInt) with Zigzag encoding
    public static byte[] encodeVarIntSigned(int value) {
        // Apply Zigzag encoding to convert signed int to unsigned
        long zigzagValue = (value << 1) ^ (value >> 31); // Zigzag formula
        
        return encodeVarInt(zigzagValue); // Encode the resulting unsigned value
    }

    // Method to encode an unsigned long as a variable-length integer (VarInt)
    public static byte[] encodeVarInt(long value) {
        List<Byte> byteList = new ArrayList<>();
        
        // While there's still data to encode
        while ((value & ~0x7F) != 0) {
            byteList.add((byte) ((value & 0x7F) | 0x80));  // Set the continuation bit
            value >>>= 7; // Unsigned right shift by 7 bits
        }
        
        byteList.add((byte) (value & 0x7F));  // Final byte, no continuation bit
        
        // Convert the list of bytes to an array
        byte[] byteArray = new byte[byteList.size()];
        for (int i = 0; i < byteList.size(); i++) {
            byteArray[i] = byteList.get(i);
        }
        
        return byteArray;
    }
}
