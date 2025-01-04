package endpoints.DescribeTopic;

import static utils.Utils.bytesToInt;
import static utils.Utils.intToBytes;
import static utils.Utils.shortToBytes;
import utils.ErrorCodes;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.OutputStream;

import endpoints.KafkaEndpoint;
import api.RequestBody;
import utils.ConstructorException;

public class DescribeTopicEndpoint implements KafkaEndpoint {

    ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();

    @Override
    public void process(RequestBody requestBody, OutputStream outputStream) throws ConstructorException, EOFException {
        responseBuffer.reset();
        // Only support 0-0 versions
        if (bytesToInt(requestBody.input_request_api_version) >= 0 && bytesToInt(requestBody.input_request_api_version) <= 0) {
            System.out.println("Handling a proper request");
            byte[] errorCode = shortToBytes((short) 0);
            byte[] throttle_time_ms = intToBytes(100);
            byte tag_buffer = 0;
            // Send data to client
            responseBuffer.write(requestBody.input_correlation_id);
            responseBuffer.write(errorCode);

            // TODO response

            responseBuffer.write(throttle_time_ms);
            responseBuffer.write(tag_buffer);
        } else {
            // Throw appropriate error code
            System.out.println("Handling a wrong request");
            byte[] errorCode = shortToBytes((short) WRONG_REQUEST_ERROR_CODE);
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
