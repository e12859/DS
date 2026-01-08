package sd.client.ui;

import sd.client.SalesClient;
import sd.common.SaleEvent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ClientConsole {
    public static void main(String[] args) {
        SalesClient client = new SalesClient("localhost", 5000);
        try {
            client.connect();
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                System.out.println("1 - Register");
                System.out.println("2 - Login");
                System.out.println("3 - Add sale");
                System.out.println("4 - Aggregate quantity");
                System.out.println("5 - Aggregate volume");
                System.out.println("6 - Aggregate average price");
                System.out.println("7 - Aggregate max price");
                System.out.println("8 - New day");
                System.out.println("9 - Filter events");
                System.out.println("10 - Wait simultaneous products");
                System.out.println("11 - Wait consecutive sales (any product)");
                System.out.println("12 - Logout");
                System.out.println("0 - Exit");
                System.out.print("Option: ");
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                line = line.trim();
                if (line.equals("0")) {
                    break;
                } else if (line.equals("1")) {
                    System.out.print("New username: ");
                    String username = reader.readLine();
                    System.out.print("New password: ");
                    String password = reader.readLine();
                    try {
                        boolean ok = client.register(username, password);
                        System.out.println(ok ? "Registered" : "Register failed");
                    } catch (IOException e) {
                        System.out.println("Error registering");
                    }
                } else if (line.equals("2")) {
                    System.out.print("Username: ");
                    String username = reader.readLine();
                    System.out.print("Password: ");
                    String password = reader.readLine();
                    try {
                        boolean ok = client.login(username, password);
                        System.out.println(ok ? "Logged in" : "Login failed");
                    } catch (IOException e) {
                        System.out.println("Error logging in");
                    }
                } else if (line.equals("3")) {
                    if (!client.isLoggedIn()) {
                        System.out.println("You must login first");
                        continue;
                    }
                    System.out.print("Product id: ");
                    String productId = reader.readLine();
                    System.out.print("Quantity: ");
                    String qLine = reader.readLine();
                    System.out.print("Price: ");
                    String pLine = reader.readLine();
                    int quantity;
                    double price;
                    try {
                        quantity = Integer.parseInt(qLine.trim());
                        price = Double.parseDouble(pLine.trim());
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid number");
                        continue;
                    }
                    try {
                        boolean ok = client.addSale(productId, quantity, price);
                        System.out.println(ok ? "Sale added" : "Add sale failed");
                    } catch (IOException e) {
                        System.out.println("Error adding sale");
                    }
                } else if (line.equals("4")) {
                    if (!client.isLoggedIn()) {
                        System.out.println("You must login first");
                        continue;
                    }
                    System.out.print("Product id: ");
                    String productId = reader.readLine();
                    System.out.print("Days (d): ");
                    String dLine = reader.readLine();
                    int days;
                    try {
                        days = Integer.parseInt(dLine.trim());
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid number");
                        continue;
                    }
                    try {
                        double result = client.aggregateQuantity(productId, days);
                        System.out.println("Quantity = " + result);
                    } catch (IOException e) {
                        System.out.println("Error aggregating");
                    }
                } else if (line.equals("5")) {
                    if (!client.isLoggedIn()) {
                        System.out.println("You must login first");
                        continue;
                    }
                    System.out.print("Product id: ");
                    String productId = reader.readLine();
                    System.out.print("Days (d): ");
                    String dLine = reader.readLine();
                    int days;
                    try {
                        days = Integer.parseInt(dLine.trim());
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid number");
                        continue;
                    }
                    try {
                        double result = client.aggregateVolume(productId, days);
                        System.out.println("Volume = " + result);
                    } catch (IOException e) {
                        System.out.println("Error aggregating");
                    }
                } else if (line.equals("6")) {
                    if (!client.isLoggedIn()) {
                        System.out.println("You must login first");
                        continue;
                    }
                    System.out.print("Product id: ");
                    String productId = reader.readLine();
                    System.out.print("Days (d): ");
                    String dLine = reader.readLine();
                    int days;
                    try {
                        days = Integer.parseInt(dLine.trim());
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid number");
                        continue;
                    }
                    try {
                        double result = client.aggregateAveragePrice(productId, days);
                        System.out.println("Average price = " + result);
                    } catch (IOException e) {
                        System.out.println("Error aggregating");
                    }
                } else if (line.equals("7")) {
                    if (!client.isLoggedIn()) {
                        System.out.println("You must login first");
                        continue;
                    }
                    System.out.print("Product id: ");
                    String productId = reader.readLine();
                    System.out.print("Days (d): ");
                    String dLine = reader.readLine();
                    int days;
                    try {
                        days = Integer.parseInt(dLine.trim());
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid number");
                        continue;
                    }
                    try {
                        double result = client.aggregateMaxPrice(productId, days);
                        System.out.println("Max price = " + result);
                    } catch (IOException e) {
                        System.out.println("Error aggregating");
                    }
                } else if (line.equals("8")) {
                    if (!client.isLoggedIn()) {
                        System.out.println("You must login first");
                        continue;
                    }
                    try {
                        boolean ok = client.nextDay();
                        System.out.println(ok ? "Day advanced" : "New day failed");
                    } catch (IOException e) {
                        System.out.println("Error advancing day");
                    }
                } else if (line.equals("9")) {
                    if (!client.isLoggedIn()) {
                        System.out.println("You must login first");
                        continue;
                    }
                    System.out.print("Days ago: ");
                    String dLine = reader.readLine();
                    int day;
                    try {
                        day = Integer.parseInt(dLine.trim());
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid number");
                        continue;
                    }
                    System.out.print("Number of products: ");
                    String nLine = reader.readLine();
                    int n;
                    try {
                        n = Integer.parseInt(nLine.trim());
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid number");
                        continue;
                    }
                    if (n < 0) {
                        System.out.println("Invalid number");
                        continue;
                    }
                    List<String> products = new ArrayList<>();
                    for (int i = 0; i < n; i++) {
                        System.out.print("Product id " + (i + 1) + ": ");
                        String p = reader.readLine();
                        products.add(p);
                    }
                    try {
                        List<SaleEvent> events = client.filterEvents(day, products);
                        System.out.println("Events: " + events.size());
                        for (SaleEvent e : events) {
                            System.out.println(e.getProductId() + " qty=" + e.getQuantity() + " price=" + e.getPrice());
                        }
                    } catch (IOException e) {
                        System.out.println("Error filtering events");
                    }
                } else if (line.equals("10")) {
                    if (!client.isLoggedIn()) {
                        System.out.println("You must login first");
                        continue;
                    }
                    System.out.print("Product 1: ");
                    String p1 = reader.readLine();
                    System.out.print("Product 2: ");
                    String p2 = reader.readLine();
                    try {
                        boolean ok = client.waitSimultaneous(p1, p2);
                        System.out.println(ok ? "Condition met" : "Day ended");
                    } catch (IOException e) {
                        System.out.println("Error waiting");
                    }
                } else if (line.equals("11")) {
                    if (!client.isLoggedIn()) {
                        System.out.println("You must login first");
                        continue;
                    }
                    System.out.print("N consecutive sales: ");
                    String cLine = reader.readLine();
                    int c;
                    try {
                        c = Integer.parseInt(cLine.trim());
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid number");
                        continue;
                    }
                    try {
                        String prod = client.waitConsecutive(c);
                        if (prod != null) {
                            System.out.println("Condition met for product: " + prod);
                        } else {
                            System.out.println("Day ended");
                        }
                    } catch (IOException e) {
                        System.out.println("Error waiting");
                    }
                } else if (line.equals("12")) {
                    if (!client.isLoggedIn()) {
                        System.out.println("Not logged in");
                        continue;
                    }
                    try {
                        boolean ok = client.logout();
                        System.out.println(ok ? "Logged out" : "Logout failed");
                    } catch (IOException e) {
                        System.out.println("Error logging out");
                    }
                } else {
                    System.out.println("Unknown option");
                }
            }
            client.close();
        } catch (IOException e) {
            System.out.println("Error connecting to server");
        }
    }
}
