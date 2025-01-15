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

    private final int API_VERSIONS_KEY = 18;
    private final int DESCRIBE_TOPIC_KEY = 75;
    private ApiMetadata[] SUPPORTED_APIs = {new ApiMetadata(API_VERSIONS_KEY, 0,4), new ApiMetadata(DESCRIBE_TOPIC_KEY,0,0)};

    // TODO make sure input stream is handled well, output stream too. No IO or EOF. readFully causes this.
    public void processAPI(DataInputStream dataInputStream, OutputStream outputStream) throws ConstructorException, IOException {
        // Receive data from client and parse
        System.out.println("inside processAPI " + this.hashCode());
        System.out.println("outputStream is " + outputStream.hashCode());
        RequestBody requestBody = new RequestBody(dataInputStream);
        System.out.println("successfully read requestBody " + this.hashCode());
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        switch (bytesToInt(requestBody.input_request_api_key)) {
            case API_VERSIONS_KEY:
                System.out.println("ApiVersions request");
                ApiVersionsEndpoint apiVersionsEndpoint = new ApiVersionsEndpoint(SUPPORTED_APIs);
                apiVersionsEndpoint.process(requestBody, responseBuffer);
                break;
            case DESCRIBE_TOPIC_KEY:
                System.out.println("DescribeTopic request");
                DescribeTopicEndpoint describeTopicEndpoint = new DescribeTopicEndpoint();
                describeTopicEndpoint.process(requestBody, responseBuffer);
                break;
            default:
                break;
        }
        byte[] responseBytes = responseBuffer.toByteArray();
        // send data to client
        System.out.println("Finishing. Writting to output stream for describe Topic endpoint");
        outputStream.write(intToBytes(responseBytes.length));
        outputStream.write(responseBytes);
        outputStream.flush();
    }
}
