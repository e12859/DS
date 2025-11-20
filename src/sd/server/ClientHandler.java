package sd.server;

import sd.common.ProtocolConstants;
import sd.common.SaleEvent;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashSet;

public class ClientHandler implements Runnable {
    private final Socket socket;
    private final UserManager userManager;
    private final SalesStore salesStore;
    private boolean loggedIn;
    private String username;

    public ClientHandler(Socket socket, UserManager userManager, SalesStore salesStore) {
        this.socket = socket;
        this.userManager = userManager;
        this.salesStore = salesStore;
        this.loggedIn = false;
        this.username = null;
    }

    @Override
    public void run() {
        try {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            while (true) {
                byte opcode;
                try {
                    opcode = in.readByte();
                } catch (EOFException e) {
                    break;
                }
                if (opcode == ProtocolConstants.MSG_LOGIN) {
                    handleLogin(in, out);
                } else if (opcode == ProtocolConstants.MSG_REGISTER) {
                    handleRegister(in, out);
                } else if (opcode == ProtocolConstants.MSG_LOGOUT) {
                    handleLogout(out);
                } else if (opcode == ProtocolConstants.MSG_ADD_SALE) {
                    if (!checkLoggedIn(out)) {
                        continue;
                    }
                    handleAddSale(in, out);
                } else if (opcode == ProtocolConstants.MSG_AGGREGATE) {
                    if (!checkLoggedIn(out)) {
                        continue;
                    }
                    handleAggregate(in, out);
                } else if (opcode == ProtocolConstants.MSG_NEW_DAY) {
                    if (!checkLoggedIn(out)) {
                        continue;
                    }
                    handleNewDay(out);
                } else if (opcode == ProtocolConstants.MSG_FILTER_EVENTS) {
                    if (!checkLoggedIn(out)) {
                        continue;
                    }
                    handleFilterEvents(in, out);
                } else if (opcode == ProtocolConstants.MSG_WAIT_SIMULTANEOUS) {
                    if (!checkLoggedIn(out)) {
                        continue;
                    }
                    handleWaitSimultaneous(in, out);
                } else if (opcode == ProtocolConstants.MSG_WAIT_CONSECUTIVE) {
                    if (!checkLoggedIn(out)) {
                        continue;
                    }
                    handleWaitConsecutive(in, out);
                } else {
                    out.writeByte(ProtocolConstants.STATUS_ERROR);
                    out.writeUTF("Unknown opcode");
                    out.flush();
                }
            }
        } catch (IOException e) {
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
            }
        }
    }

    private boolean checkLoggedIn(DataOutputStream out) throws IOException {
        if (!loggedIn) {
            out.writeByte(ProtocolConstants.STATUS_ERROR);
            out.writeUTF("Not logged in");
            out.flush();
            return false;
        }
        return true;
    }

    private void handleLogin(DataInputStream in, DataOutputStream out) throws IOException {
        String user = in.readUTF();
        String password = in.readUTF();
        boolean ok = userManager.authenticate(user, password);
        if (ok) {
            loggedIn = true;
            username = user;
            out.writeByte(ProtocolConstants.STATUS_OK);
        } else {
            loggedIn = false;
            username = null;
            out.writeByte(ProtocolConstants.STATUS_ERROR);
            out.writeUTF("Invalid credentials");
        }
        out.flush();
    }

    private void handleRegister(DataInputStream in, DataOutputStream out) throws IOException {
        String user = in.readUTF();
        String password = in.readUTF();
        boolean ok = userManager.register(user, password);
        if (ok) {
            out.writeByte(ProtocolConstants.STATUS_OK);
        } else {
            out.writeByte(ProtocolConstants.STATUS_ERROR);
            out.writeUTF("User already exists");
        }
        out.flush();
    }

    private void handleLogout(DataOutputStream out) throws IOException {
        if (!loggedIn) {
            out.writeByte(ProtocolConstants.STATUS_ERROR);
            out.writeUTF("Not logged in");
        } else {
            loggedIn = false;
            username = null;
            out.writeByte(ProtocolConstants.STATUS_OK);
        }
        out.flush();
    }

    private void handleAddSale(DataInputStream in, DataOutputStream out) throws IOException {
        String productId = in.readUTF();
        int quantity = in.readInt();
        double price = in.readDouble();
        salesStore.addSale(productId, quantity, price);
        out.writeByte(ProtocolConstants.STATUS_OK);
        out.flush();
    }

    private void handleAggregate(DataInputStream in, DataOutputStream out) throws IOException {
        byte aggType = in.readByte();
        String productId = in.readUTF();
        int lastDays = in.readInt();
        if (lastDays <= 0) {
            out.writeByte(ProtocolConstants.STATUS_ERROR);
            out.writeUTF("Invalid lastDays");
            out.flush();
            return;
        }
        double result;
        if (aggType == ProtocolConstants.AGG_QUANTITY) {
            result = salesStore.aggregateQuantity(productId, lastDays);
        } else if (aggType == ProtocolConstants.AGG_VOLUME) {
            result = salesStore.aggregateVolume(productId, lastDays);
        } else if (aggType == ProtocolConstants.AGG_AVG_PRICE) {
            result = salesStore.aggregateAveragePrice(productId, lastDays);
        } else if (aggType == ProtocolConstants.AGG_MAX_PRICE) {
            result = salesStore.aggregateMaxPrice(productId, lastDays);
        } else {
            out.writeByte(ProtocolConstants.STATUS_ERROR);
            out.writeUTF("Unknown aggregate type");
            out.flush();
            return;
        }
        out.writeByte(ProtocolConstants.STATUS_OK);
        out.writeDouble(result);
        out.flush();
    }

    private void handleNewDay(DataOutputStream out) throws IOException {
        salesStore.nextDay();
        out.writeByte(ProtocolConstants.STATUS_OK);
        out.flush();
    }

    private void handleFilterEvents(DataInputStream in, DataOutputStream out) throws IOException {
        int day = in.readInt();
        int numProducts = in.readInt();
        if (numProducts < 0) {
            out.writeByte(ProtocolConstants.STATUS_ERROR);
            out.writeUTF("Invalid product count");
            out.flush();
            return;
        }
        List<String> products = new ArrayList<>();
        for (int i = 0; i < numProducts; i++) {
            String p = in.readUTF();
            products.add(p);
        }
        List<SaleEvent> events = salesStore.filterEvents(day, products);
        LinkedHashSet<String> set = new LinkedHashSet<>();
        for (SaleEvent e : events) {
            set.add(e.getProductId());
        }
        List<String> distinctProducts = new ArrayList<>(set);
        Map<String, Integer> indexMap = new HashMap<>();
        for (int i = 0; i < distinctProducts.size(); i++) {
            indexMap.put(distinctProducts.get(i), i);
        }
        out.writeByte(ProtocolConstants.STATUS_OK);
        out.writeInt(distinctProducts.size());
        for (String p : distinctProducts) {
            out.writeUTF(p);
        }
        out.writeInt(events.size());
        for (SaleEvent e : events) {
            int idx = indexMap.get(e.getProductId());
            out.writeInt(idx);
            out.writeInt(e.getQuantity());
            out.writeDouble(e.getPrice());
        }
        out.flush();
    }

    private void handleWaitSimultaneous(DataInputStream in, DataOutputStream out) throws IOException {
        String product1 = in.readUTF();
        String product2 = in.readUTF();
        try {
            boolean result = salesStore.waitForSimultaneous(product1, product2);
            out.writeByte(ProtocolConstants.STATUS_OK);
            out.writeBoolean(result);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            out.writeByte(ProtocolConstants.STATUS_ERROR);
            out.writeUTF("Interrupted");
        }
        out.flush();
    }

    private void handleWaitConsecutive(DataInputStream in, DataOutputStream out) throws IOException {
        int count = in.readInt();
        if (count <= 0) {
            out.writeByte(ProtocolConstants.STATUS_ERROR);
            out.writeUTF("Invalid count");
            out.flush();
            return;
        }
        try {
            String product = salesStore.waitForConsecutive(count);
            out.writeByte(ProtocolConstants.STATUS_OK);
            if (product != null) {
                out.writeBoolean(true);
                out.writeUTF(product);
            } else {
                out.writeBoolean(false);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            out.writeByte(ProtocolConstants.STATUS_ERROR);
            out.writeUTF("Interrupted");
        }
        out.flush();
    }
}
