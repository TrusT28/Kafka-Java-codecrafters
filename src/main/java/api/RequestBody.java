package api;
import static utils.Utils.bytesToInt;
    
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import utils.ConstructorException;

public class RequestBody {
        public byte[] input_message_size = new byte[4];
        public byte[] input_request_api_key = new byte[2];
        public byte[] input_request_api_version = new byte[2];
        public byte[] input_correlation_id = new byte[4];
        public byte[] input_client_id_length = new byte[2];
        public byte[] input_client_id = null;
        public byte[] body = null;

        public RequestBody(DataInputStream dataInputStream) throws ConstructorException, EOFException{
            try {
                System.out.println("dataInputStream legnth "+dataInputStream.available());
                dataInputStream.readFully(input_message_size);
                dataInputStream.readFully(input_request_api_key);
                dataInputStream.readFully(input_request_api_version);
                dataInputStream.readFully(input_correlation_id);

                int requestHeaderSize = input_request_api_key.length + input_request_api_version.length + input_correlation_id.length;

                if(bytesToInt(input_request_api_key) == ApiCodes.DESCRIBE_TOPIC_KEY) {
                    dataInputStream.readFully(input_client_id_length);
                    input_client_id = new byte[bytesToInt(input_client_id_length)];
                    dataInputStream.readFully(input_client_id);
                    // Tag Buffer
                    dataInputStream.read();
                    requestHeaderSize += input_client_id_length.length + input_client_id.length + 1;
                }
                
                body = new byte[bytesToInt(input_message_size) - requestHeaderSize];
                dataInputStream.readFully(body);
            }
            catch(java.io.EOFException e) {
                System.out.println("Unexpected EOF when reading Request Body");
                throw new ConstructorException(e.getMessage());
            }
            catch(IOException e) {
                throw new ConstructorException("Failed to create InputData: "+ e);
            }

        }
}