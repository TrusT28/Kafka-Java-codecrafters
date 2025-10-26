package endpoints.Produce;

import api.RequestBody;
import endpoints.Fetch.models.FetchRequestBody;
import endpoints.KafkaEndpoint;
import endpoints.Produce.models.ProduceRequestBody;
import utils.ConstructorException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ProduceEndpoint implements KafkaEndpoint {
    @Override
    public void process(RequestBody requestBody, ByteArrayOutputStream outputStream) throws IOException, ConstructorException {
        System.out.println("Handling Produce request");
        byte tag_buffer = 0;
        ByteArrayInputStream bodyStream = new ByteArrayInputStream(requestBody.body);
        // Parse request body
        ProduceRequestBody produceRequestBody = new ProduceRequestBody(bodyStream);

    }
}
