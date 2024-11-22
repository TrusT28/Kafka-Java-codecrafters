import api.API;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;

public class Client implements Runnable {
    private final Socket clientSocket;

    public Client(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        System.out.println("Got a client!");
        API api = new API();
        // Determine the call endpoint and call it
        while (true) {
            try {
                api.apiVersionsEndpoint(clientSocket);
                System.out.println("API Processed!");
            } catch (EOFException e) {
                System.out.println("Client disconnected: " + e.getMessage());
                break; // Exit the loop when client disconnects
            } catch (IOException e) {
                System.err.println("Error processing client request: " + e.getMessage());
                break; // Exit the loop on any IO error
            } finally {
                if (clientSocket != null && !clientSocket.isClosed()) {
                    try {
                        System.out.println("Closing the client!");
                        clientSocket.close();
                    } catch (IOException e) {
                        System.out.println("IOException: " + e.getMessage());
                    }
                }
            }
        }
    }
}
