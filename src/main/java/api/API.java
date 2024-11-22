package api;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;

import static utils.Utils.*;

public class API {

    private final int WRONG_REQUEST_ERROR_CODE = 35;

    public void apiVersionsEndpoint(Socket clientSocket) throws IOException {
        // Receive data from client and parse
        OutputStream outputStream = clientSocket.getOutputStream();
        try (DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());
             ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream()
        ) {
            byte[] input_message_size = new byte[4];
            byte[] input_request_api_key = new byte[2];
            byte[] input_request_api_version = new byte[2];
            byte[] input_correlation_id = new byte[4];
            dataInputStream.readFully(input_message_size);
            dataInputStream.readFully(input_request_api_key);
            dataInputStream.readFully(input_request_api_version);
            dataInputStream.readFully(input_correlation_id);
            // Only support ApiVersions request
            int API_VERSIONS_KEY = 18;
            if (bytesToInt(input_request_api_key) == API_VERSIONS_KEY) {
                // Only support 0-4 versions
                if (bytesToInt(input_request_api_version) >= 0 && bytesToInt(input_request_api_version) <= 4) {
                    System.out.println("Handling a proper request");
                    byte[] errorCode = shortToBytes((short) 0);
                    byte apiKeysArrayDefinition = (byte) 2; // 1 element in COMPACT ARRAY + 1 for N+1 encoding
                    byte[] apiVersion = shortToBytes((short) API_VERSIONS_KEY);
                    byte[] minVersion = shortToBytes((short) 0);
                    byte[] maxVersion = shortToBytes((short) 4);
                    byte[] throttle_time_ms = intToBytes(100);
                    byte tag_buffer = 0;
                    // Send data to client
                    responseBuffer.write(input_correlation_id);
                    responseBuffer.write(errorCode);

                    responseBuffer.write(apiKeysArrayDefinition);
                    responseBuffer.write(apiVersion);
                    responseBuffer.write(minVersion);
                    responseBuffer.write(maxVersion);
                    responseBuffer.write(tag_buffer);

                    responseBuffer.write(throttle_time_ms);
                    responseBuffer.write(tag_buffer);
                } else {
                    // Throw appropriate error code
                    System.out.println("Handling a wrong request");
                    byte[] errorCode = shortToBytes((short) WRONG_REQUEST_ERROR_CODE);
                    // specifies the size of the header and body.
                    responseBuffer.write(input_correlation_id);
                    responseBuffer.write(errorCode);
                }
                byte[] responseBytes = responseBuffer.toByteArray();
                outputStream.write(intToBytes(responseBytes.length));
                outputStream.write(responseBytes);
                outputStream.flush();
            }
        } catch (EOFException e) {
            System.err.println("Client disconnected abruptly: " + e.getMessage());
        } catch (SocketException e) {
            System.err.println("Socket error with client: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("I/O error while processing client: " + e.getMessage());
        } finally {
            System.out.println("Cleaning up after client...");
        }
    }
}
