package sd.server;

public class ServerMain {

    public static void main(String[] args) {
        int port = 5000;
        int D = 7;
        int S = 3;
        String dataDir = "data";

        if (args != null && args.length > 0) {
            if (args.length == 1 && ("--help".equals(args[0]) || "-h".equals(args[0]))) {
                printUsage(0);
                return;
            }

            if (args.length != 4) {
                System.err.println("Invalid arguments.");
                printUsage(2);
                return;
            }

            try {
                port = Integer.parseInt(args[0]);
                D = Integer.parseInt(args[1]);
                S = Integer.parseInt(args[2]);
                dataDir = args[3];
            } catch (NumberFormatException e) {
                System.err.println("Invalid number format in arguments.");
                printUsage(2);
                return;
            }
        }

        String err = validate(port, D, S, dataDir);
        if (err != null) {
            System.err.println("Invalid server configuration: " + err);
            printUsage(2);
            return;
        }

        SalesServer server = new SalesServer(port, D, S, dataDir);
        try {
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static String validate(int port, int D, int S, String dataDir) {
        if (port < 1 || port > 65535) return "port must be in [1, 65535]";
        if (D < 1) return "D must be >= 1";
        if (S < 0) return "S must be >= 0";
        if (S >= D) return "S must satisfy S < D";
        if (dataDir == null || dataDir.trim().isEmpty()) return "dataDir must be non-empty";
        return null;
    }

    private static void printUsage(int code) {
        String msg = "Usage: java sd.server.ServerMain [port D S dataDir]  (or --help)";
        if (code == 0) System.out.println(msg);
        else System.err.println(msg);
        System.exit(code);
    }
}
