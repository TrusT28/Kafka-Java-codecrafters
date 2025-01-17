package api;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;

import endpoints.ApiVersions.ApiVersionsEndpoint;
import endpoints.DescribeTopic.DescribeTopicEndpoint;
import utils.ConstructorException;

import static utils.Utils.*;

public class API {
    private ApiMetadata[] SUPPORTED_APIs = {new ApiMetadata(ApiCodes.API_VERSIONS_KEY, 0,4), new ApiMetadata(ApiCodes.DESCRIBE_TOPIC_KEY,0,0)};

    // TODO make sure input stream is handled well, output stream too. No IO or EOF. readFully causes this.
    public void processAPI(DataInputStream dataInputStream, OutputStream outputStream) throws ConstructorException, IOException {
        // Receive data from client and parse
        RequestBody requestBody = new RequestBody(dataInputStream);
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        System.out.println("Received request with correlation id " + bytesToInt(requestBody.input_correlation_id));
        switch (bytesToInt(requestBody.input_request_api_key)) {
            case ApiCodes.API_VERSIONS_KEY:
                System.out.println("ApiVersions request");
                ApiVersionsEndpoint apiVersionsEndpoint = new ApiVersionsEndpoint(SUPPORTED_APIs);
                apiVersionsEndpoint.process(requestBody, responseBuffer);
                break;
            case ApiCodes.DESCRIBE_TOPIC_KEY:
                System.out.println("DescribeTopic request");
                DescribeTopicEndpoint describeTopicEndpoint = new DescribeTopicEndpoint();
                describeTopicEndpoint.process(requestBody, responseBuffer);
                break;
            default:
                break;
        }
        byte[] responseBytes = responseBuffer.toByteArray();
        System.out.println("Final response is of size " + intToBytes(responseBytes.length));
        // send data to client
        outputStream.write(intToBytes(responseBytes.length));
        outputStream.write(responseBytes);
        outputStream.flush();
    }
}
