package api;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;

import utils.ConstructorException;
import utils.InputData;

import static utils.Utils.*;

public class API {

    private final int WRONG_REQUEST_ERROR_CODE = 35;
    private final int API_VERSIONS_KEY = 18;
    private final int DESCRIBE_VERSIONS_KEY = 75;
    private ApiMetadata[] SUPPORTED_APIs = {new ApiMetadata(API_VERSIONS_KEY, 0,4), new ApiMetadata(DESCRIBE_VERSIONS_KEY,0,0)};
    ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();

    public void processAPI(DataInputStream dataInputStream, OutputStream outputStream) throws ConstructorException, IOException {
        // Receive data from client and parse
        responseBuffer.reset();
        InputData inputData = new InputData(dataInputStream);
        System.out.println("Body length is " + inputData.body.length);
        switch (bytesToInt(inputData.input_request_api_key)) {
            case API_VERSIONS_KEY:
                apiVersionsEndpoint(inputData, outputStream);
                break;
            case DESCRIBE_VERSIONS_KEY:

                break;
            default:
                break;
        }
       
    }

    public void apiVersionsEndpoint(InputData inputData, OutputStream outputStream) throws IOException{
            // Only support 0-4 versions
            if (bytesToInt(inputData.input_request_api_version) >= 0 && bytesToInt(inputData.input_request_api_version) <= 4) {
                System.out.println("Handling a proper request");
                byte[] errorCode = shortToBytes((short) 0);
                byte apiKeysArrayDefinition = (byte) (SUPPORTED_APIs.length+1); // 1 element in COMPACT ARRAY + 1 for N+1 encoding
                byte[] throttle_time_ms = intToBytes(100);
                byte tag_buffer = 0;

                // Send data to client
                responseBuffer.write(inputData.input_correlation_id);
                responseBuffer.write(errorCode);
                responseBuffer.write(apiKeysArrayDefinition);

                for(int i=0; i<SUPPORTED_APIs.length; i++) {
                    byte[] apiVersion = shortToBytes((short) SUPPORTED_APIs[i].key);
                    byte[] minVersion = shortToBytes((short) SUPPORTED_APIs[i].minVersion);
                    byte[] maxVersion = shortToBytes((short) SUPPORTED_APIs[i].maxVersion);
                    responseBuffer.write(apiVersion);
                    responseBuffer.write(minVersion);
                    responseBuffer.write(maxVersion);
                    responseBuffer.write(tag_buffer);
                }

                responseBuffer.write(throttle_time_ms);
                responseBuffer.write(tag_buffer);
            } else {
                // Throw appropriate error code
                System.out.println("Handling a wrong request");
                byte[] errorCode = shortToBytes((short) WRONG_REQUEST_ERROR_CODE);
                // specifies the size of the header and body.
                responseBuffer.write(inputData.input_correlation_id);
                responseBuffer.write(errorCode);
            }
            byte[] responseBytes = responseBuffer.toByteArray();
            outputStream.write(intToBytes(responseBytes.length));
            outputStream.write(responseBytes);
            outputStream.flush();
    }

    // TODO write implementation
    public void DescribeVersionsEndpoint(InputData inputData, OutputStream outputStream) throws IOException{
        // Only support 0-0 versions
        if (bytesToInt(inputData.input_request_api_version) >= 0 && bytesToInt(inputData.input_request_api_version) <= 0) {
            System.out.println("Handling a proper request");
            byte[] errorCode = shortToBytes((short) 0);
            byte apiKeysArrayDefinition = (byte) 2; // 1 element in COMPACT ARRAY + 1 for N+1 encoding
            byte[] apiVersion = shortToBytes((short) API_VERSIONS_KEY);
            byte[] minVersion = shortToBytes((short) 0);
            byte[] maxVersion = shortToBytes((short) 4);
            byte[] throttle_time_ms = intToBytes(100);
            byte tag_buffer = 0;
            // Send data to client
            responseBuffer.write(inputData.input_correlation_id);
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
            responseBuffer.write(inputData.input_correlation_id);
            responseBuffer.write(errorCode);
        }
        byte[] responseBytes = responseBuffer.toByteArray();
        outputStream.write(intToBytes(responseBytes.length));
        outputStream.write(responseBytes);
        outputStream.flush();
}
}
