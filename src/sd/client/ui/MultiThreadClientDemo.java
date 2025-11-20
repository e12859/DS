package sd.client.ui;

import sd.client.SalesClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class MultiThreadClientDemo {
    public static void main(String[] args) {
        SalesClient client = new SalesClient("localhost", 5000);
        try {
            client.connect();
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            System.out.print("Username: ");
            String username = reader.readLine();
            System.out.print("Password: ");
            String password = reader.readLine();
            boolean ok = client.login(username, password);
            if (!ok) {
                System.out.println("Login failed");
                client.close();
                return;
            }
            System.out.print("Number of threads: ");
            String tLine = reader.readLine();
            System.out.print("Operations per thread: ");
            String oLine = reader.readLine();
            int threads;
            int ops;
            try {
                threads = Integer.parseInt(tLine.trim());
                ops = Integer.parseInt(oLine.trim());
            } catch (NumberFormatException e) {
                System.out.println("Invalid number");
                client.close();
                return;
            }
            if (threads <= 0 || ops <= 0) {
                System.out.println("Invalid values");
                client.close();
                return;
            }
            Thread[] workers = new Thread[threads];
            long start = System.currentTimeMillis();
            for (int i = 0; i < threads; i++) {
                int id = i;
                workers[i] = new Thread(() -> runWorker(client, id, ops));
                workers[i].start();
            }
            for (int i = 0; i < threads; i++) {
                try {
                    workers[i].join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            long end = System.currentTimeMillis();
            System.out.println("All threads finished in " + (end - start) + " ms");
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void runWorker(SalesClient client, int id, int ops) {
        String productBase = "p" + id;
        for (int i = 0; i < ops; i++) {
            String productId = productBase;
            int quantity = 1;
            double price = 1.0 + id;
            try {
                client.addSale(productId, quantity, price);
                if (i % 10 == 0) {
                    client.aggregateQuantity(productId, 1);
                }
            } catch (IOException e) {
                System.out.println("Worker " + id + " error: " + e.getMessage());
                return;
            }
        }
        System.out.println("Worker " + id + " done");
    }
}
