package api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import static utils.Utils.*;

public class API {

    private final int WRONG_REQUEST_ERROR_CODE = 35;

    public void apiVersionsEndpoint(Socket clientSocket) throws IOException {
        // Receive data from client and parse
        InputStream inputStream = clientSocket.getInputStream();
        OutputStream outputStream = clientSocket.getOutputStream();
        byte[] input_message_size = new byte[4];
        inputStream.read(input_message_size);

        byte[] input_request_api_key = new byte[2];
        inputStream.read(input_request_api_key);

        byte[] input_request_api_version = new byte[2];
        inputStream.read(input_request_api_version);

        byte[] input_correlation_id = new byte[4];
        inputStream.read(input_correlation_id);

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
                // specifies the size of the header and body.
                byte[] message_size = intToBytes(input_correlation_id.length + errorCode.length + apiVersion.length + minVersion.length + maxVersion.length + throttle_time_ms.length + 3);

                // Send data to client
                outputStream.write(message_size);
                outputStream.write(input_correlation_id);
                outputStream.write(errorCode);

                outputStream.write(apiKeysArrayDefinition);
                outputStream.write(apiVersion);
                outputStream.write(minVersion);
                outputStream.write(maxVersion);
                outputStream.write(tag_buffer);

                outputStream.write(throttle_time_ms);
                outputStream.write(tag_buffer);
            } else {
                // Throw appropriate error code
                System.out.println("Handling a wrong request");
                byte[] errorCode = shortToBytes((short) WRONG_REQUEST_ERROR_CODE);
                // specifies the size of the header and body.
                byte[] message_size = intToBytes(6);
                outputStream.write(message_size);
                outputStream.write(input_correlation_id);
                outputStream.write(errorCode);
            }
        }
        System.out.println("Closing the streams");
        outputStream.close();
        inputStream.close();
        System.out.println("streams closed");
    }
}
