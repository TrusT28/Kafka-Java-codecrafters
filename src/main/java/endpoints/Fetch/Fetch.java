package endpoints.Fetch;

import static utils.Utils.bytesToInt;
import static utils.Utils.encodeVarInt;
import static utils.Utils.intToBytes;
import static utils.Utils.shortToBytes;
import utils.ErrorCodes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import api.ApiMetadata;
import api.RequestBody;
import endpoints.KafkaEndpoint;

public class Fetch implements KafkaEndpoint{

    @Override
    public void process(RequestBody requestBody, ByteArrayOutputStream responseBuffer) throws IOException {
            // Only support 0-16 versions
            if (bytesToInt(requestBody.input_request_api_version) >= 0 && bytesToInt(requestBody.input_request_api_version) <= 16) {
                System.out.println("Handling a proper request");
                byte[] errorCode = shortToBytes((short) ErrorCodes.NO_ERROR);
                byte[] throttle_time_ms = intToBytes(0);
                byte tag_buffer = 0;

                // Send data to client
                // Response Header
                responseBuffer.write(requestBody.input_correlation_id);
                responseBuffer.write(tag_buffer);

                // Fetch Body
                responseBuffer.write(throttle_time_ms);
                responseBuffer.write(errorCode);
                // Session Id
                responseBuffer.write(intToBytes(0));
                // Responses
                responseBuffer.write(encodeVarInt(0));
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
