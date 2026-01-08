package sd.client.ui;

import sd.client.SalesClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BenchmarkSuite {

    private static final class StartSignal {
        private boolean started;

        synchronized void awaitStart() throws InterruptedException {
            while (!started) {
                wait();
            }
        }

        synchronized void start() {
            started = true;
            notifyAll();
        }
    }

    private static final class DoneSignal {
        private int remaining;

        DoneSignal(int remaining) {
            this.remaining = remaining;
        }

        synchronized void done() {
            remaining--;
            if (remaining <= 0) {
                notifyAll();
            }
        }

        synchronized void awaitDone() throws InterruptedException {
            while (remaining > 0) {
                wait();
            }
        }
    }

    private static void usage() {
        System.out.println("Usage: java sd.client.ui.BenchmarkSuite <host> <port> <user> <pass> <clients> <threads> <opsPerThread>");
    }

    private static void ensureUser(SalesClient client, String user, String pass) throws IOException {
        client.register(user, pass);
        if (!client.login(user, pass)) {
            throw new IOException("Login failed");
        }
    }

    private static double opsPerSecond(long ops, long nanos) {
        if (nanos <= 0) return 0.0;
        return (ops * 1_000_000_000.0) / nanos;
    }

    private static long runMultiThreadSingleConnection(final SalesClient client, int threads, final int opsPerThread) throws InterruptedException {
        final StartSignal start = new StartSignal();
        final DoneSignal done = new DoneSignal(threads);
        final List<Throwable> errors = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        start.awaitStart();
                        for (int i = 0; i < opsPerThread; i++) {
                            String product = "p" + ((tid * 997 + i) % 20);
                            int qty = 1 + ((i + tid) % 5);
                            double price = 1.0 + ((i + tid) % 10);
                            client.addSale(product, qty, price);
                            if ((i % 50) == 0) {
                                client.aggregateVolume(product, 7);
                            }
                        }
                    } catch (Throwable e) {
                        synchronized (errors) {
                            errors.add(e);
                        }
                    } finally {
                        done.done();
                    }
                }
            }, "bench-single-" + t);
            th.setDaemon(true);
            th.start();
        }

        long t0 = System.nanoTime();
        start.start();
        done.awaitDone();
        long t1 = System.nanoTime();

        if (!errors.isEmpty()) {
            Throwable e = errors.get(0);
            if (e instanceof RuntimeException) throw (RuntimeException) e;
            throw new RuntimeException(e);
        }

        return t1 - t0;
    }

    private static long runMultiClient(final int clients, final String host, final int port, final String user, final String pass, final int opsPerClient) throws InterruptedException {
        final StartSignal start = new StartSignal();
        final DoneSignal done = new DoneSignal(clients);
        final List<Throwable> errors = new ArrayList<>();

        for (int c = 0; c < clients; c++) {
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    SalesClient client = new SalesClient(host, port);
                    try {
                        client.connect();
                        ensureUser(client, user, pass);
                        start.awaitStart();
                        for (int i = 0; i < opsPerClient; i++) {
                            String product = "c" + ((i * 31) % 20);
                            client.addSale(product, 1 + (i % 3), 2.0 + (i % 7));
                            if ((i % 40) == 0) {
                                client.aggregateQuantity(product, 7);
                            }
                        }
                    } catch (Throwable e) {
                        synchronized (errors) {
                            errors.add(e);
                        }
                    } finally {
                        try {
                            client.close();
                        } catch (IOException e) {
                        }
                        done.done();
                    }
                }
            }, "bench-client-" + c);
            th.setDaemon(true);
            th.start();
        }

        long t0 = System.nanoTime();
        start.start();
        done.awaitDone();
        long t1 = System.nanoTime();

        if (!errors.isEmpty()) {
            Throwable e = errors.get(0);
            if (e instanceof RuntimeException) throw (RuntimeException) e;
            throw new RuntimeException(e);
        }

        return t1 - t0;
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 7) {
            usage();
            return;
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String user = args[2];
        String pass = args[3];
        int clients = Integer.parseInt(args[4]);
        int threads = Integer.parseInt(args[5]);
        int opsPerThread = Integer.parseInt(args[6]);

        if (port <= 0 || port > 65535) throw new IllegalArgumentException("Invalid port");
        if (clients <= 0) throw new IllegalArgumentException("clients must be > 0");
        if (threads <= 0) throw new IllegalArgumentException("threads must be > 0");
        if (opsPerThread <= 0) throw new IllegalArgumentException("opsPerThread must be > 0");

        SalesClient shared = new SalesClient(host, port);
        shared.connect();
        ensureUser(shared, user, pass);

        long ops1 = (long) threads * (long) opsPerThread;
        long nanos1 = runMultiThreadSingleConnection(shared, threads, opsPerThread);
        System.out.println("Test 1: single connection, " + threads + " threads, ops=" + ops1 + ", timeMs=" + (nanos1 / 1_000_000.0) + ", ops/s=" + opsPerSecond(ops1, nanos1));

        shared.close();

        int opsPerClient = threads * opsPerThread;
        long ops2 = (long) clients * (long) opsPerClient;
        long nanos2 = runMultiClient(clients, host, port, user, pass, opsPerClient);
        System.out.println("Test 2: " + clients + " clients, ops=" + ops2 + ", timeMs=" + (nanos2 / 1_000_000.0) + ", ops/s=" + opsPerSecond(ops2, nanos2));
    }
}
