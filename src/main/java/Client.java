import api.API;

import java.io.IOException;
import java.net.Socket;

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
            System.out.println("API Processed!");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (clientSocket != null) {
                    System.out.println("Closing the client!");
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}
