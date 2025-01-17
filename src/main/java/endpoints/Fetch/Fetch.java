package endpoints.Fetch;

import static utils.Utils.bytesToInt;
import static utils.Utils.encodeVarInt;
import static utils.Utils.intToBytes;
import static utils.Utils.shortToBytes;

import utils.ConstructorException;
import utils.ErrorCodes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import api.ApiMetadata;
import api.RequestBody;
import endpoints.KafkaEndpoint;
import endpoints.Fetch.models.FetchRequestBody;
import endpoints.Fetch.models.FetchRequestTopics;

public class Fetch implements KafkaEndpoint{

    @Override
    public void process(RequestBody requestBody, ByteArrayOutputStream responseBuffer) throws IOException {
            // Only support 0-16 versions
            if (bytesToInt(requestBody.input_request_api_version) >= 0 && bytesToInt(requestBody.input_request_api_version) <= 16) {
                System.out.println("Handling a proper request");
                byte tag_buffer = 0;
                ByteArrayInputStream bodyStream = new ByteArrayInputStream(requestBody.body);
                // Parse request body
                FetchRequestBody fetchRequestBody;
                try {
                    fetchRequestBody = new FetchRequestBody(bodyStream);
                }
                catch(ConstructorException e) {
                    System.out.println("Failed to read Fetch Request body");
                    return;
                }
                catch(Exception e) {
                    System.out.println("Failed to read Fetch Request body. Unexpected error " + e.getStackTrace());
                    return;
                }
                // Write response

                // Response Header
                responseBuffer.write(requestBody.input_correlation_id);
                responseBuffer.write(tag_buffer);

                writeResponseBody(fetchRequestBody, responseBuffer);
            } else {
                // Throw appropriate error code
                System.out.println("Handling a wrong request");
                byte[] errorCode = shortToBytes((short) ErrorCodes.WRONG_REQUEST_ERROR_CODE);
                // specifies the size of the header and body.
                responseBuffer.write(requestBody.input_correlation_id);
                responseBuffer.write(errorCode);
            }
    }

    private void writeResponseBody(FetchRequestBody requestBody, ByteArrayOutputStream responseBuffer) throws IOException {
        byte[] errorCode = shortToBytes((short) ErrorCodes.NO_ERROR);
        byte[] throttle_time_ms = intToBytes(0);
        byte tag_buffer = 0;

        responseBuffer.write(throttle_time_ms);
        responseBuffer.write(errorCode);
        // Session Id
        responseBuffer.write(intToBytes(0));
        // Responses
        System.out.println("Response length is " + requestBody.topicsArrayLength);
        if(requestBody.topicsArrayLength>1) {
            // Responses Length
            responseBuffer.write(encodeVarInt(requestBody.topicsArrayLength));
            for(FetchRequestTopics topics : requestBody.topics) {
                // Topic ID
                responseBuffer.write(topics.topicUUID);
                // TODO Partitions hardcoded
                    // Length
                    responseBuffer.write(encodeVarInt(2));
                    // Partitions index
                    responseBuffer.write(intToBytes(0));
                    // Error code
                    responseBuffer.write(shortToBytes(ErrorCodes.NO_ERROR));
                responseBuffer.write(tag_buffer);
            }
        }
        else {
            // Responses length 0
            System.out.println("Response length was 0");
            responseBuffer.write(encodeVarInt(0));
        }
        responseBuffer.write(tag_buffer);
    }
}
