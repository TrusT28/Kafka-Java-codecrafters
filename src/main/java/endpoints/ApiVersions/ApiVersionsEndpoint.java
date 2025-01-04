package endpoints.ApiVersions;

import static utils.Utils.bytesToInt;
import static utils.Utils.intToBytes;
import static utils.Utils.shortToBytes;
import utils.ErrorCodes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import api.ApiMetadata;
import api.RequestBody;
import endpoints.KafkaEndpoint;

public class ApiVersionsEndpoint implements KafkaEndpoint{
    ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
    private ApiMetadata[] SUPPORTED_APIs = null;

    public ApiVersionsEndpoint(ApiMetadata[] SUPPORTED_APIs) {
        this.SUPPORTED_APIs = SUPPORTED_APIs;
    }

    @Override
    public void process(RequestBody requestBody, OutputStream outputStream) throws IOException {
            // Only support 0-4 versions
            if (bytesToInt(requestBody.input_request_api_version) >= 0 && bytesToInt(requestBody.input_request_api_version) <= 4) {
                System.out.println("Handling a proper request");
                byte[] errorCode = shortToBytes((short) 0);
                byte apiKeysArrayDefinition = (byte) (SUPPORTED_APIs.length+1); // 1 element in COMPACT ARRAY + 1 for N+1 encoding
                byte[] throttle_time_ms = intToBytes(100);
                byte tag_buffer = 0;

                // Send data to client
                responseBuffer.write(requestBody.input_correlation_id);
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
                byte[] errorCode = shortToBytes((short) ErrorCodes.WRONG_REQUEST_ERROR_CODE);
                // specifies the size of the header and body.
                responseBuffer.write(requestBody.input_correlation_id);
                responseBuffer.write(errorCode);
            }
            byte[] responseBytes = responseBuffer.toByteArray();
            outputStream.write(intToBytes(responseBytes.length));
            outputStream.write(responseBytes);
            outputStream.flush();
    }
}
