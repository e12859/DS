package sd.client;

import sd.common.ProtocolConstants;
import sd.common.SaleEvent;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class SalesClient {
    private final String host;
    private final int port;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private boolean loggedIn;

    public SalesClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.loggedIn = false;
    }

    public synchronized void connect() throws IOException {
        if (socket != null) {
            return;
        }
        socket = new Socket(host, port);
        in = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());
    }

    public synchronized boolean register(String username, String password) throws IOException {
        out.writeByte(ProtocolConstants.MSG_REGISTER);
        out.writeUTF(username);
        out.writeUTF(password);
        out.flush();
        byte status = in.readByte();
        if (status == ProtocolConstants.STATUS_OK) {
            return true;
        } else {
            String message = in.readUTF();
            return false;
        }
    }

    public synchronized boolean login(String username, String password) throws IOException {
        out.writeByte(ProtocolConstants.MSG_LOGIN);
        out.writeUTF(username);
        out.writeUTF(password);
        out.flush();
        byte status = in.readByte();
        if (status == ProtocolConstants.STATUS_OK) {
            loggedIn = true;
            return true;
        } else {
            String message = in.readUTF();
            return false;
        }
    }

    public synchronized boolean logout() throws IOException {
        out.writeByte(ProtocolConstants.MSG_LOGOUT);
        out.flush();
        byte status = in.readByte();
        if (status == ProtocolConstants.STATUS_OK) {
            loggedIn = false;
            return true;
        } else {
            String message = in.readUTF();
            return false;
        }
    }

    public synchronized boolean isLoggedIn() {
        return loggedIn;
    }

    public synchronized boolean addSale(String productId, int quantity, double price) throws IOException {
        out.writeByte(ProtocolConstants.MSG_ADD_SALE);
        out.writeUTF(productId);
        out.writeInt(quantity);
        out.writeDouble(price);
        out.flush();
        byte status = in.readByte();
        if (status == ProtocolConstants.STATUS_OK) {
            return true;
        } else {
            String message = in.readUTF();
            return false;
        }
    }

    private synchronized double aggregate(byte aggType, String productId, int lastDays) throws IOException {
        out.writeByte(ProtocolConstants.MSG_AGGREGATE);
        out.writeByte(aggType);
        out.writeUTF(productId);
        out.writeInt(lastDays);
        out.flush();
        byte status = in.readByte();
        if (status == ProtocolConstants.STATUS_OK) {
            return in.readDouble();
        } else {
            String message = in.readUTF();
            return 0.0;
        }
    }

    public double aggregateQuantity(String productId, int lastDays) throws IOException {
        return aggregate(ProtocolConstants.AGG_QUANTITY, productId, lastDays);
    }

    public double aggregateVolume(String productId, int lastDays) throws IOException {
        return aggregate(ProtocolConstants.AGG_VOLUME, productId, lastDays);
    }

    public double aggregateAveragePrice(String productId, int lastDays) throws IOException {
        return aggregate(ProtocolConstants.AGG_AVG_PRICE, productId, lastDays);
    }

    public double aggregateMaxPrice(String productId, int lastDays) throws IOException {
        return aggregate(ProtocolConstants.AGG_MAX_PRICE, productId, lastDays);
    }

    public synchronized boolean nextDay() throws IOException {
        out.writeByte(ProtocolConstants.MSG_NEW_DAY);
        out.flush();
        byte status = in.readByte();
        if (status == ProtocolConstants.STATUS_OK) {
            return true;
        } else {
            String message = in.readUTF();
            return false;
        }
    }

    public synchronized List<SaleEvent> filterEvents(int day, List<String> productIds) throws IOException {
        out.writeByte(ProtocolConstants.MSG_FILTER_EVENTS);
        out.writeInt(day);
        out.writeInt(productIds.size());
        for (String p : productIds) {
            out.writeUTF(p);
        }
        out.flush();
        byte status = in.readByte();
        if (status == ProtocolConstants.STATUS_OK) {
            int numProducts = in.readInt();
            List<String> productsTable = new ArrayList<>();
            for (int i = 0; i < numProducts; i++) {
                String pid = in.readUTF();
                productsTable.add(pid);
            }
            int numEvents = in.readInt();
            List<SaleEvent> result = new ArrayList<>();
            for (int i = 0; i < numEvents; i++) {
                int productIndex = in.readInt();
                int quantity = in.readInt();
                double price = in.readDouble();
                String productId = productsTable.get(productIndex);
                SaleEvent e = new SaleEvent(productId, quantity, price, day);
                result.add(e);
            }
            return result;
        } else {
            String message = in.readUTF();
            return new ArrayList<>();
        }
    }

    public synchronized boolean waitSimultaneous(String product1, String product2) throws IOException {
        out.writeByte(ProtocolConstants.MSG_WAIT_SIMULTANEOUS);
        out.writeUTF(product1);
        out.writeUTF(product2);
        out.flush();
        byte status = in.readByte();
        if (status == ProtocolConstants.STATUS_OK) {
            boolean result = in.readBoolean();
            return result;
        } else {
            String message = in.readUTF();
            return false;
        }
    }

    public synchronized String waitConsecutive(int count) throws IOException {
        out.writeByte(ProtocolConstants.MSG_WAIT_CONSECUTIVE);
        out.writeInt(count);
        out.flush();
        byte status = in.readByte();
        if (status == ProtocolConstants.STATUS_OK) {
            boolean found = in.readBoolean();
            if (found) {
                String productId = in.readUTF();
                return productId;
            } else {
                return null;
            }
        } else {
            String message = in.readUTF();
            return null;
        }
    }

    public synchronized void close() throws IOException {
        if (socket != null) {
            socket.close();
            socket = null;
            in = null;
            out = null;
            loggedIn = false;
        }
    }
}
