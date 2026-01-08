package sd.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class SalesServer {
    private final int port;
    private final UserManager userManager;
    private final SalesStore salesStore;

    public SalesServer(int port, int maxDays, int maxCached, String dataDir) {
        this.port = port;
        this.userManager = new UserManager(dataDir);
        this.salesStore = new SalesStore(maxDays, maxCached, dataDir);
    }

    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                ClientHandler handler = new ClientHandler(clientSocket, userManager, salesStore);
                Thread t = new Thread(handler);
                t.start();
            }
        }
    }
}
