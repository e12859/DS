package sd.client;

import sd.common.ProtocolConstants;
import sd.common.SaleEvent;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SalesClient {
    private final String host;
    private final int port;

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    private final Object writeLock = new Object();
    private final Object pendingLock = new Object();
    private final Map<Integer, Pending> pending = new HashMap<>();

    private volatile boolean loggedIn;
    private volatile boolean closed;
    private int nextRequestId = 1;
    private Thread readerThread;

    private void logIOException(String where, IOException e) {
        System.err.println(where + ": " + e.getMessage());
        e.printStackTrace(System.err);
    }

    private interface RequestWriter {
        void write(DataOutputStream out) throws IOException;
    }

    private interface ResponseParser {
        Object parse(DataInputStream in) throws IOException;
    }

    private static final class Pending {
        final ResponseParser parser;
        boolean done;
        byte status;
        Object value;

        Pending(ResponseParser parser) {
            this.parser = parser;
        }
    }

    public SalesClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.loggedIn = false;
        this.closed = false;
    }

    public synchronized void connect() throws IOException {
        if (socket != null) return;

        socket = new Socket(host, port);
        in = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());

        closed = false;
        loggedIn = false;
        nextRequestId = 1;

        readerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                readLoop();
            }
        });
        readerThread.setDaemon(true);
        readerThread.start();
    }

    public boolean isLoggedIn() {
        return loggedIn;
    }

    private synchronized int newRequestId() {
        return nextRequestId++;
    }

    private Pending send(byte opcode, RequestWriter writer, ResponseParser parser) throws IOException {
        if (socket == null || out == null || in == null) throw new IOException("Not connected");
        if (closed) throw new IOException("Closed");

        int reqId = newRequestId();
        Pending p = new Pending(parser);

        synchronized (pendingLock) {
            pending.put(reqId, p);
        }

        try {
            synchronized (writeLock) {
                out.writeInt(reqId);
                out.writeByte(opcode);
                if (writer != null) writer.write(out);
                out.flush();
            }
        } catch (IOException e) {
            synchronized (pendingLock) {
                pending.remove(reqId);
            }
            throw e;
        }

        synchronized (p) {
            while (!p.done) {
                try {
                    p.wait();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    synchronized (pendingLock) {
                        pending.remove(reqId);
                    }
                    throw new IOException("Interrupted");
                }
            }
        }

        return p;
    }

    private void finishAllPendingAsClosed() {
        List<Pending> toNotify = new ArrayList<>();
        synchronized (pendingLock) {
            toNotify.addAll(pending.values());
            pending.clear();
        }
        for (Pending p : toNotify) {
            synchronized (p) {
                p.status = ProtocolConstants.STATUS_ERROR;
                p.done = true;
                p.notifyAll();
            }
        }
    }

    private void readLoop() {
        try {
            while (!closed) {
                int reqId;
                byte status;
                try {
                    reqId = in.readInt();
                    status = in.readByte();
                } catch (EOFException eof) {
                    break;
                }

                Pending p;
                synchronized (pendingLock) {
                    p = pending.remove(reqId);
                }

                if (p == null) {
                    closed = true;
                    break;
                }

                Object value = null;

                if (status == ProtocolConstants.STATUS_OK) {
                    if (p.parser != null) {
                        value = p.parser.parse(in);
                    }
                } else {
                    try {
                        in.readUTF();
                    } catch (IOException e) {
                        logIOException("SalesClient.readLoop.readErrorMessage", e);
                    }
                }

                synchronized (p) {
                    p.status = status;
                    p.value = value;
                    p.done = true;
                    p.notifyAll();
                }
            }
        } catch (IOException e) {
            logIOException("SalesClient.readLoop", e);
        } finally {
            closed = true;
            finishAllPendingAsClosed();
            try {
                if (socket != null) socket.close();
            } catch (IOException e) {
                logIOException("SalesClient.readLoop.close", e);
            }
        }
    }

    public boolean register(final String username, final String password) throws IOException {
        Pending p = send(ProtocolConstants.MSG_REGISTER, new RequestWriter() {
            @Override
            public void write(DataOutputStream out) throws IOException {
                out.writeUTF(username);
                out.writeUTF(password);
            }
        }, null);
        return p.status == ProtocolConstants.STATUS_OK;
    }

    public boolean login(final String username, final String password) throws IOException {
        Pending p = send(ProtocolConstants.MSG_LOGIN, new RequestWriter() {
            @Override
            public void write(DataOutputStream out) throws IOException {
                out.writeUTF(username);
                out.writeUTF(password);
            }
        }, null);
        boolean ok = p.status == ProtocolConstants.STATUS_OK;
        if (ok) loggedIn = true;
        return ok;
    }

    public boolean logout() throws IOException {
        Pending p = send(ProtocolConstants.MSG_LOGOUT, null, null);
        boolean ok = p.status == ProtocolConstants.STATUS_OK;
        if (ok) loggedIn = false;
        return ok;
    }

    public boolean addSale(final String productId, final int quantity, final double price) throws IOException {
        Pending p = send(ProtocolConstants.MSG_ADD_SALE, new RequestWriter() {
            @Override
            public void write(DataOutputStream out) throws IOException {
                out.writeUTF(productId);
                out.writeInt(quantity);
                out.writeDouble(price);
            }
        }, null);
        return p.status == ProtocolConstants.STATUS_OK;
    }

    private double aggregate(final byte aggType, final String productId, final int lastDays) throws IOException {
        Pending p = send(ProtocolConstants.MSG_AGGREGATE, new RequestWriter() {
            @Override
            public void write(DataOutputStream out) throws IOException {
                out.writeByte(aggType);
                out.writeUTF(productId);
                out.writeInt(lastDays);
            }
        }, new ResponseParser() {
            @Override
            public Object parse(DataInputStream in) throws IOException {
                return in.readDouble();
            }
        });

        if (p.status != ProtocolConstants.STATUS_OK) return 0.0;
        return ((Double) p.value).doubleValue();
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

    public boolean nextDay() throws IOException {
        Pending p = send(ProtocolConstants.MSG_NEW_DAY, null, null);
        return p.status == ProtocolConstants.STATUS_OK;
    }

    public List<SaleEvent> filterEvents(final int day, final List<String> productIds) throws IOException {
        Pending p = send(ProtocolConstants.MSG_FILTER_EVENTS, new RequestWriter() {
            @Override
            public void write(DataOutputStream out) throws IOException {
                out.writeInt(day);
                out.writeInt(productIds.size());
                for (String prod : productIds) out.writeUTF(prod);
            }
        }, new ResponseParser() {
            @Override
            public Object parse(DataInputStream in) throws IOException {
                int numProducts = in.readInt();
                List<String> productsTable = new ArrayList<>();
                for (int i = 0; i < numProducts; i++) productsTable.add(in.readUTF());

                int numEvents = in.readInt();
                List<SaleEvent> result = new ArrayList<>();
                for (int i = 0; i < numEvents; i++) {
                    int productIndex = in.readInt();
                    int quantity = in.readInt();
                    double price = in.readDouble();
                    String productId = productsTable.get(productIndex);
                    result.add(new SaleEvent(productId, quantity, price, day));
                }
                return result;
            }
        });

        if (p.status != ProtocolConstants.STATUS_OK) return new ArrayList<>();
        @SuppressWarnings("unchecked")
        List<SaleEvent> res = (List<SaleEvent>) p.value;
        return res;
    }

    public boolean waitSimultaneous(final String product1, final String product2) throws IOException {
        Pending p = send(ProtocolConstants.MSG_WAIT_SIMULTANEOUS, new RequestWriter() {
            @Override
            public void write(DataOutputStream out) throws IOException {
                out.writeUTF(product1);
                out.writeUTF(product2);
            }
        }, new ResponseParser() {
            @Override
            public Object parse(DataInputStream in) throws IOException {
                return in.readBoolean();
            }
        });

        if (p.status != ProtocolConstants.STATUS_OK) return false;
        return ((Boolean) p.value).booleanValue();
    }

    public String waitConsecutive(final int count) throws IOException {
        Pending p = send(ProtocolConstants.MSG_WAIT_CONSECUTIVE, new RequestWriter() {
            @Override
            public void write(DataOutputStream out) throws IOException {
                out.writeInt(count);
            }
        }, new ResponseParser() {
            @Override
            public Object parse(DataInputStream in) throws IOException {
                boolean found = in.readBoolean();
                if (found) return in.readUTF();
                return null;
            }
        });

        if (p.status != ProtocolConstants.STATUS_OK) return null;
        return (String) p.value;
    }

    public synchronized void close() throws IOException {
        closed = true;
        finishAllPendingAsClosed();
        if (socket != null) {
            socket.close();
            socket = null;
            in = null;
            out = null;
            loggedIn = false;
        }
    }
}
