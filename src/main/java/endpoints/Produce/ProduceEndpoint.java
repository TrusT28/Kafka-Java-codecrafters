package endpoints.Produce;

import api.RequestBody;
import endpoints.KafkaEndpoint;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ProduceEndpoint implements KafkaEndpoint {
    @Override
    public void process(RequestBody requestBody, ByteArrayOutputStream outputStream) throws IOException {

    }
}
