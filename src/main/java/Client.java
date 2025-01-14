import api.API;
import utils.ConstructorException;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class Client implements Runnable {
    private final Socket clientSocket;

    public Client(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try {
            System.out.println("Got a client!");
            API api = new API();
            // Determine the call endpoint and call it
            DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());
            OutputStream outputStream = clientSocket.getOutputStream();
            while (true) {
                try {
                    System.out.println("API in progress!");
                    api.processAPI(dataInputStream, outputStream);
                    System.out.println("API Processed!");
                }
                catch(EOFException e) {
                    System.err.println("EOF. Client closed connection." + e.getMessage());
                    break;
                }
                catch(IOException e) {
                    System.err.println("IO. Client closed connection."  + e.getMessage());
                    break;
                }
                catch (ConstructorException e) {
                    System.err.println("Error processing client request. Constructor failed to parse input data: " + e.getMessage());
                    break;
                }
                catch (Throwable e) {
                    System.out.println("Something went wrong: " + e.getMessage());
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println("I/O error with client: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null && !clientSocket.isClosed()) {
                    System.out.println("Closing the client! " + this.hashCode());
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
    }
}
