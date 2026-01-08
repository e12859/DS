package sd.server;

import sd.common.ProtocolConstants;
import sd.common.SaleEvent;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

public class ClientHandler implements Runnable {
    private static final int TASK_QUEUE_CAPACITY = 4096;
    private static final SimpleThreadPool workerPool = new SimpleThreadPool(8, TASK_QUEUE_CAPACITY);

    private final Socket socket;
    private final UserManager userManager;
    private final SalesStore salesStore;

    private volatile boolean loggedIn;
    private volatile boolean running;

    private DataInputStream in;
    private DataOutputStream out;

    private final Object outLock = new Object();

    private void logIOException(String where, IOException e) {
        System.err.println(where + ": " + e.getMessage());
        e.printStackTrace(System.err);
    }

    private static boolean isValidNonEmpty(String s) {
        return s != null && !s.trim().isEmpty();
    }

    private static boolean isValidPrice(double price) {
        return !(Double.isNaN(price) || Double.isInfinite(price) || price < 0.0);
    }

    private static final class SimpleThreadPool {
        private final int capacity;
        private final ArrayDeque<Runnable> tasks;
        private final Thread[] workers;

        SimpleThreadPool(int nThreads, int capacity) {
            this.capacity = Math.max(1, capacity);
            this.tasks = new ArrayDeque<>();
            this.workers = new Thread[nThreads];
            for (int i = 0; i < nThreads; i++) {
                workers[i] = new Thread(new Worker(), "worker-" + i);
                workers[i].setDaemon(true);
                workers[i].start();
            }
        }

        boolean execute(Runnable r) {
            synchronized (tasks) {
                if (tasks.size() >= capacity) return false;
                tasks.addLast(r);
                tasks.notifyAll();
                return true;
            }
        }

        private final class Worker implements Runnable {
            @Override
            public void run() {
                while (true) {
                    Runnable r;
                    synchronized (tasks) {
                        while (tasks.isEmpty()) {
                            try {
                                tasks.wait();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                        r = tasks.removeFirst();
                    }
                    try {
                        r.run();
                    } catch (Throwable t) {
                        System.err.println("ClientHandler.worker: " + t.getMessage());
                        t.printStackTrace(System.err);
                    }
                }
            }
        }
    }

    public ClientHandler(Socket socket, UserManager userManager, SalesStore salesStore) {
        this.socket = socket;
        this.userManager = userManager;
        this.salesStore = salesStore;
        this.loggedIn = false;
        this.running = true;
    }

    private void submitOrBusy(int requestId, Runnable r) {
        if (!workerPool.execute(r)) {
            sendError(requestId, "Server busy");
        }
    }

    @Override
    public void run() {
        try {
            in = new DataInputStream(socket.getInputStream());
            out = new DataOutputStream(socket.getOutputStream());

            while (running) {
                int requestId;
                byte opcode;
                try {
                    requestId = in.readInt();
                    opcode = in.readByte();
                } catch (EOFException e) {
                    break;
                }

                switch (opcode) {
                    case ProtocolConstants.MSG_LOGIN: {
                        final int req = requestId;
                        final String user = in.readUTF();
                        final String pass = in.readUTF();

                        if (!isValidNonEmpty(user) || !isValidNonEmpty(pass)) {
                            sendError(req, "Invalid credentials");
                            break;
                        }

                        boolean ok = userManager.authenticate(user, pass);
                        if (ok) {
                            loggedIn = true;
                            sendOk(req);
                        } else {
                            sendError(req, "Invalid credentials");
                        }
                        break;
                    }

                    case ProtocolConstants.MSG_REGISTER: {
                        final int req = requestId;
                        final String user = in.readUTF();
                        final String pass = in.readUTF();

                        if (!isValidNonEmpty(user) || !isValidNonEmpty(pass)) {
                            sendError(req, "Invalid registration data");
                            break;
                        }

                        boolean ok = userManager.register(user, pass);
                        if (ok) {
                            sendOk(req);
                        } else {
                            sendError(req, "User already exists");
                        }
                        break;
                    }

                    case ProtocolConstants.MSG_LOGOUT: {
                        final int req = requestId;

                        if (!loggedIn) {
                            sendError(req, "Not logged in");
                            break;
                        }

                        loggedIn = false;
                        sendOk(req);
                        break;
                    }

                    case ProtocolConstants.MSG_ADD_SALE: {
                        final int req = requestId;
                        final String productIdRaw = in.readUTF();
                        final int quantity = in.readInt();
                        final double price = in.readDouble();

                        if (!loggedIn) {
                            sendError(req, "Not logged in");
                            break;
                        }

                        final String productId = (productIdRaw == null) ? null : productIdRaw.trim();
                        if (!isValidNonEmpty(productId) || quantity <= 0 || !isValidPrice(price)) {
                            sendError(req, "Invalid sale data");
                            break;
                        }

                        submitOrBusy(req, new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    salesStore.addSale(productId, quantity, price);
                                    sendOk(req);
                                } catch (IllegalArgumentException e) {
                                    sendError(req, "Invalid sale data");
                                } catch (IllegalStateException e) {
                                    sendError(req, "I/O error");
                                } catch (RuntimeException e) {
                                    sendError(req, "Server error");
                                }
                            }
                        });
                        break;
                    }

                    case ProtocolConstants.MSG_NEW_DAY: {
                        final int req = requestId;

                        if (!loggedIn) {
                            sendError(req, "Not logged in");
                            break;
                        }

                        submitOrBusy(req, new Runnable() {
                            @Override
                            public void run() {
                                salesStore.nextDay();
                                sendOk(req);
                            }
                        });
                        break;
                    }

                    case ProtocolConstants.MSG_AGGREGATE: {
                        final int req = requestId;
                        final byte aggType = in.readByte();
                        final String productIdRaw = in.readUTF();
                        final int lastDays = in.readInt();

                        if (!loggedIn) {
                            sendError(req, "Not logged in");
                            break;
                        }

                        final String productId = (productIdRaw == null) ? null : productIdRaw.trim();
                        if (!isValidNonEmpty(productId) || lastDays <= 0) {
                            sendError(req, "Invalid aggregation parameters");
                            break;
                        }

                        submitOrBusy(req, new Runnable() {
                            @Override
                            public void run() {
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
                                    sendError(req, "Unknown aggregation type");
                                    return;
                                }
                                sendOkDouble(req, result);
                            }
                        });
                        break;
                    }

                    case ProtocolConstants.MSG_FILTER_EVENTS: {
                        final int req = requestId;
                        final int day = in.readInt();
                        int n = in.readInt();
                        final List<String> products = new ArrayList<>();
                        for (int i = 0; i < n; i++) products.add(in.readUTF());

                        if (!loggedIn) {
                            sendError(req, "Not logged in");
                            break;
                        }

                        submitOrBusy(req, new Runnable() {
                            @Override
                            public void run() {
                                List<SaleEvent> events = salesStore.filterEvents(day, products);

                                LinkedHashSet<String> productsTableSet = new LinkedHashSet<>();
                                for (SaleEvent e : events) productsTableSet.add(e.getProductId());
                                final List<String> productsTable = new ArrayList<>(productsTableSet);

                                final Map<String, Integer> index = new HashMap<>();
                                for (int i = 0; i < productsTable.size(); i++) index.put(productsTable.get(i), i);

                                synchronized (outLock) {
                                    if (!running) return;
                                    try {
                                        out.writeInt(req);
                                        out.writeByte(ProtocolConstants.STATUS_OK);

                                        out.writeInt(productsTable.size());
                                        for (String p : productsTable) out.writeUTF(p);

                                        out.writeInt(events.size());
                                        for (SaleEvent e : events) {
                                            out.writeInt(index.get(e.getProductId()));
                                            out.writeInt(e.getQuantity());
                                            out.writeDouble(e.getPrice());
                                        }

                                        out.flush();
                                    } catch (IOException e) {
                                        logIOException("ClientHandler.sendFilterEvents", e);
                                        closeNow();
                                    } catch (RuntimeException e) {
                                        closeNow();
                                    }
                                }
                            }
                        });
                        break;
                    }

                    case ProtocolConstants.MSG_WAIT_SIMULTANEOUS: {
                        final int req = requestId;
                        final String p1Raw = in.readUTF();
                        final String p2Raw = in.readUTF();

                        if (!loggedIn) {
                            sendError(req, "Not logged in");
                            break;
                        }

                        final String p1 = (p1Raw == null) ? null : p1Raw.trim();
                        final String p2 = (p2Raw == null) ? null : p2Raw.trim();
                        if (!isValidNonEmpty(p1) || !isValidNonEmpty(p2)) {
                            sendError(req, "Invalid productId");
                            break;
                        }

                        submitOrBusy(req, new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    boolean result = salesStore.waitForSimultaneous(p1, p2);
                                    sendOkBoolean(req, result);
                                } catch (InterruptedException e) {
                                    sendError(req, "Interrupted");
                                } catch (IllegalArgumentException e) {
                                    sendError(req, "Invalid productId");
                                } catch (RuntimeException e) {
                                    sendError(req, "Server error");
                                }
                            }
                        });
                        break;
                    }

                    case ProtocolConstants.MSG_WAIT_CONSECUTIVE: {
                        final int req = requestId;
                        final int count = in.readInt();

                        if (!loggedIn) {
                            sendError(req, "Not logged in");
                            break;
                        }

                        if (count <= 0) {
                            sendError(req, "Invalid count");
                            break;
                        }

                        submitOrBusy(req, new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    String product = salesStore.waitForConsecutive(count);
                                    sendOkConsecutive(req, product);
                                } catch (InterruptedException e) {
                                    sendError(req, "Interrupted");
                                } catch (IllegalArgumentException e) {
                                    sendError(req, "Invalid count");
                                } catch (RuntimeException e) {
                                    sendError(req, "Server error");
                                }
                            }
                        });
                        break;
                    }

                    default:
                        sendError(requestId, "Unknown opcode");
                        break;
                }
            }
        } catch (IOException e) {
            logIOException("ClientHandler.run", e);
        } finally {
            closeNow();
        }
    }

    private void closeNow() {
        running = false;
        try {
            socket.close();
        } catch (IOException e) {
            logIOException("ClientHandler.closeNow", e);
        }
    }

    private void sendOk(final int requestId) {
        synchronized (outLock) {
            if (!running) return;
            try {
                out.writeInt(requestId);
                out.writeByte(ProtocolConstants.STATUS_OK);
                out.flush();
            } catch (IOException e) {
                logIOException("ClientHandler.sendOk", e);
                closeNow();
            }
        }
    }

    private void sendOkDouble(final int requestId, final double v) {
        synchronized (outLock) {
            if (!running) return;
            try {
                out.writeInt(requestId);
                out.writeByte(ProtocolConstants.STATUS_OK);
                out.writeDouble(v);
                out.flush();
            } catch (IOException e) {
                logIOException("ClientHandler.sendOkDouble", e);
                closeNow();
            }
        }
    }

    private void sendOkBoolean(final int requestId, final boolean v) {
        synchronized (outLock) {
            if (!running) return;
            try {
                out.writeInt(requestId);
                out.writeByte(ProtocolConstants.STATUS_OK);
                out.writeBoolean(v);
                out.flush();
            } catch (IOException e) {
                logIOException("ClientHandler.sendOkBoolean", e);
                closeNow();
            }
        }
    }

    private void sendOkConsecutive(final int requestId, final String product) {
        synchronized (outLock) {
            if (!running) return;
            try {
                out.writeInt(requestId);
                out.writeByte(ProtocolConstants.STATUS_OK);
                if (product != null) {
                    out.writeBoolean(true);
                    out.writeUTF(product);
                } else {
                    out.writeBoolean(false);
                }
                out.flush();
            } catch (IOException e) {
                logIOException("ClientHandler.sendOkConsecutive", e);
                closeNow();
            }
        }
    }

    private void sendError(final int requestId, final String msg) {
        synchronized (outLock) {
            if (!running) return;
            try {
                out.writeInt(requestId);
                out.writeByte(ProtocolConstants.STATUS_ERROR);
                out.writeUTF(msg);
                out.flush();
            } catch (IOException e) {
                logIOException("ClientHandler.sendError", e);
                closeNow();
            }
        }
    }
}
