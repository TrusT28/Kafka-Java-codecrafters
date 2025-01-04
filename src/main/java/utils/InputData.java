package utils;

import static utils.Utils.bytesToInt;

import java.io.DataInputStream;
import java.io.IOException;

public class InputData {
    public byte[] input_message_size = new byte[4];
    public byte[] input_request_api_key = new byte[2];
    public byte[] input_request_api_version = new byte[2];
    public byte[] input_correlation_id = new byte[4];
    public byte[] body = null;

    public InputData(DataInputStream dataInputStream) throws ConstructorException{
        try {
            dataInputStream.readFully(input_message_size);
            dataInputStream.readFully(input_request_api_key);
            dataInputStream.readFully(input_request_api_version);
            dataInputStream.readFully(input_correlation_id);
            body = new byte[bytesToInt(input_message_size) - input_request_api_key.length - input_request_api_version.length - input_correlation_id.length];
            dataInputStream.readFully(body);
        }
        catch(IOException e) {
            throw new ConstructorException("Failed to create InputData: "+ e);
        }
    }
}
