package api;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;

import endpoints.DescribeTopic.DescribeTopicEndpoint;
import utils.ConstructorException;

import static utils.Utils.*;

public class API {

    private final int WRONG_REQUEST_ERROR_CODE = 35;
    private final int API_VERSIONS_KEY = 18;
    private final int DESCRIBE_TOPIC_KEY = 75;
    private ApiMetadata[] SUPPORTED_APIs = {new ApiMetadata(API_VERSIONS_KEY, 0,4), new ApiMetadata(DESCRIBE_TOPIC_KEY,0,0)};
    ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();

    public void processAPI(DataInputStream dataInputStream, OutputStream outputStream) throws ConstructorException, IOException {
        // Receive data from client and parse
        responseBuffer.reset();
        System.out.println("Body length is " + inputData.body.length);
        switch (bytesToInt(inputData.input_request_api_key)) {
            case API_VERSIONS_KEY:
                System.out.println("ApiVersions request");
                apiVersionsEndpoint(inputData, outputStream);
                break;
            case DESCRIBE_TOPIC_KEY:
                System.out.println("DescribeTopic request");
                DescribeTopicEndpoint describeTopicEndpoint = new DescribeTopicEndpoint();
                describeTopicEndpoint.process(dataInputStream, outputStream);
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
                    byte[] apiKey = shortToBytes((short) SUPPORTED_APIs[i].key);
                    byte[] minVersion = shortToBytes((short) SUPPORTED_APIs[i].minVersion);
                    byte[] maxVersion = shortToBytes((short) SUPPORTED_APIs[i].maxVersion);
                    responseBuffer.write(apiKey);
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
}
