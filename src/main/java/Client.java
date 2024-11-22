import java.io.IOException;
import java.net.Socket;
import api.API;

public class Client implements Runnable {
    private final Socket clientSocket;

    public Client(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try {
        System.out.println("Got a client !");
        API api = new API();
        // Determine the call endpoint and call it
        api.apiVersionsEndpoint(clientSocket);
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}
