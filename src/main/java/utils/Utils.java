package utils;

import java.nio.ByteBuffer;

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
}
