package endpoints.ApiVersions;

import static utils.Utils.bytesToInt;
import static utils.Utils.intToBytes;
import static utils.Utils.shortToBytes;
import utils.ErrorCodes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import api.ApiMetadata;
import api.RequestBody;
import endpoints.KafkaEndpoint;

public class ApiVersionsEndpoint implements KafkaEndpoint{
    private ApiMetadata[] SUPPORTED_APIs = null;

    public ApiVersionsEndpoint(ApiMetadata[] SUPPORTED_APIs) {
        this.SUPPORTED_APIs = SUPPORTED_APIs;
    }

    @Override
    public void process(RequestBody requestBody, ByteArrayOutputStream responseBuffer) throws IOException {
            // Only support 0-4 versions
            if (bytesToInt(requestBody.input_request_api_version) >= 0 && bytesToInt(requestBody.input_request_api_version) <= 4) {
                System.out.println("Handling a proper request");
                byte[] errorCode = shortToBytes((short) ErrorCodes.NO_ERROR);
                byte apiKeysArrayDefinition = (byte) (SUPPORTED_APIs.length+1); // 1 element in COMPACT ARRAY + 1 for N+1 encoding
                byte[] throttle_time_ms = intToBytes(100);
                byte tag_buffer = 0;

                // Send data to client
                responseBuffer.write(requestBody.input_correlation_id);
                responseBuffer.write(errorCode);
                responseBuffer.write(apiKeysArrayDefinition);

                for (ApiMetadata supportedApi : SUPPORTED_APIs) {
                    byte[] apiKey = shortToBytes((short) supportedApi.key);
                    byte[] minVersion = shortToBytes((short) supportedApi.minVersion);
                    byte[] maxVersion = shortToBytes((short) supportedApi.maxVersion);
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
    }
}
