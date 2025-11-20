package sd.server;

public class ServerMain {
    public static void main(String[] args) {
        int port = 5000;
        int maxDays = 7;
        int maxCached = 3;
        String dataDir = "data";
        SalesServer server = new SalesServer(port, maxDays, maxCached, dataDir);
        try {
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
