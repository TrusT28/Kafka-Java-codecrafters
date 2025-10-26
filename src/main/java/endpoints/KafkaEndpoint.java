package endpoints;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import api.RequestBody;
import utils.ConstructorException;

public interface KafkaEndpoint {
    public void process(RequestBody requestBody, ByteArrayOutputStream outputStream) throws IOException, ConstructorException;
}
