import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) {
        final ExecutorService clientProcessingPool = Executors.newFixedThreadPool(10);

        Runnable serverTask = () -> {
            try(ServerSocket serverSocket = new ServerSocket(9092)) {
                // Since the tester restarts your program quite often, setting SO_REUSEADDR
                // ensures that we don't run into 'Address already in use' errors
                serverSocket.setReuseAddress(true);
                while (true) {
                    try {
                        Socket clientSocket = serverSocket.accept();
                        clientProcessingPool.submit(new Client(clientSocket));
                    } catch (IOException e) {
                        System.out.println("Error accepting client connection" + e);
                        break;
                    }
                }
                System.out.println("While loop is over");
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            } finally {
                clientProcessingPool.shutdown();
                System.out.println("Server stopped");
            }
        };
        new Thread(serverTask).start();
    }

}
