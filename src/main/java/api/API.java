package api;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
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
    ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();

    // TODO make sure input stream is handled well, output stream too. No IO or EOF. readFully causes this.
    public void processAPI(DataInputStream dataInputStream, OutputStream outputStream) {
        // Receive data from client and parse
        System.out.println("inside processAPI " + this.hashCode());
        System.out.println("outputStream is " + outputStream.hashCode());
        responseBuffer.reset();
        RequestBody requestBody;
        try {
            requestBody = new RequestBody(dataInputStream);
        } catch (EOFException | ConstructorException e) {
            System.out.println("Failed to process input Request body");
            e.printStackTrace();
            return;
        }
        System.out.println("successfully read requestBody " + this.hashCode());
        switch (bytesToInt(requestBody.input_request_api_key)) {
            case API_VERSIONS_KEY:
                System.out.println("ApiVersions request");
                ApiVersionsEndpoint apiVersionsEndpoint = new ApiVersionsEndpoint(SUPPORTED_APIs);
                try {
                    apiVersionsEndpoint.process(requestBody, outputStream);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    System.out.println("Failed to process describeTopicEndpoint. " + e.getMessage());
                    e.printStackTrace();
                }
                break;
            case DESCRIBE_TOPIC_KEY:
                System.out.println("DescribeTopic request");
                DescribeTopicEndpoint describeTopicEndpoint = new DescribeTopicEndpoint();
                try {
                    describeTopicEndpoint.process(requestBody, outputStream);
                } catch (IOException e) {
                    System.out.println("Failed to process describeTopicEndpoint. " + e.getMessage());
                    e.printStackTrace();
                }
                break;
            default:
                break;
        }
       
    }
}
