import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.err.println("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    // 
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 9092;
    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      // Wait for connection from client.
      clientSocket = serverSocket.accept();

      apiVersionsEndpoint(clientSocket);
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }

  public static void apiVersionsEndpoint(Socket clientSocket) throws IOException {
      // Receive data from client and parse
      InputStream inputStream = clientSocket.getInputStream();
      byte[] input_message_size = new byte[4];
      inputStream.read(input_message_size);

      byte[] input_request_api_key = new byte[2];
      inputStream.read(input_request_api_key);

      byte[] input_request_api_version = new byte[2];
      inputStream.read(input_request_api_version);

      byte[] input_correlation_id = new byte[4];
      inputStream.read(input_correlation_id);

      // Only support ApiVersions request
      if (bytesToInt(input_request_api_key) == 18) {
        OutputStream outputStream = clientSocket.getOutputStream();
        // Only support 0-4 versions
        if(bytesToInt(input_request_api_version)>=0 && bytesToInt(input_request_api_version)<=4) {
          System.out.println("Handling a proper request");
          byte[] errorCode = shortToBytes((short) 0);
          byte[] apiVersion = shortToBytes((short) 18);
          byte[] minVersion = shortToBytes((short) 0);
          byte[] maxVersion = shortToBytes((short) 4);
          byte [] throttle_time_ms = intToBytes(0);

          byte[] message_size = intToBytes(errorCode.length+apiVersion.length+minVersion.length+maxVersion.length+throttle_time_ms.length-5);
          System.out.println("Message_size is" + message_size);
          // Send data to client
          outputStream.write(message_size);
          outputStream.write(input_correlation_id);
          outputStream.write(errorCode);
          outputStream.write(apiVersion);
          outputStream.write(minVersion);
          outputStream.write(maxVersion);
          outputStream.write(throttle_time_ms);
        }
        else {
          // Throw appropriate error code
          System.out.println("Handling a wrong request");
          byte[] errorCode = shortToBytes((short) 35);
          byte[] message_size = intToBytes(errorCode.length-1);
          System.out.println("Message_size is" + message_size);
          outputStream.write(message_size);
          outputStream.write(input_correlation_id);
          outputStream.write(errorCode);
        }
      }
  }

  public static int bytesToInt(byte[] bytes) {
    if (bytes.length == 4) {
      return ByteBuffer.wrap(bytes).getInt();
    }
    else if(bytes.length == 2) {
      return (int) ByteBuffer.wrap(bytes).getShort();
    }
    else {
      throw new IllegalArgumentException("Array must contain exactly 2 or 4 bytes");
    }
  }

  public static byte[] shortToBytes(short value) {
    byte[] result =  ByteBuffer.allocate(2).putShort(value).array();
    System.out.println("Length is " + result.length);
    return result;
  }

  public static byte[] intToBytes(int value) {
    byte[] result =  ByteBuffer.allocate(4).putInt(value).array();
    System.out.println("Length is " + result.length);
    return result;
  }

}
