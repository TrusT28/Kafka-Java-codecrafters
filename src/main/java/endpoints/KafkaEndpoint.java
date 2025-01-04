package endpoints;

import java.io.IOException;
import java.io.OutputStream;

import api.RequestBody;

public interface KafkaEndpoint {
    public void process(RequestBody requestBody, OutputStream outputStream) throws IOException;
}
