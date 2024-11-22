import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) {
        final ExecutorService clientProcessingPool = Executors.newFixedThreadPool(10);

        Runnable serverTask = new Runnable() {
            @Override
            public void run() {
                ServerSocket serverSocket = null;
                int port = 9092;
                try {
                    serverSocket = new ServerSocket(port);
                    // Since the tester restarts your program quite often, setting SO_REUSEADDR
                    // ensures that we don't run into 'Address already in use' errors
                    serverSocket.setReuseAddress(true);
                    while(true) {
                        // Wait for connection from client.
                        Socket clientSocket = serverSocket.accept();
                        clientProcessingPool.submit(new Client(clientSocket));
                    }
                } catch (IOException e) {
                    System.out.println("IOException: " + e.getMessage());
                }
            }
        };
    }

}
