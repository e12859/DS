package sd.server;

import sd.common.SaleEvent;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SalesStore {
    private static final int WRITE_BUFFER_SIZE = 64 * 1024;

    private final int maxDays;
    private final int totalDays;
    private final int maxCached;
    private final File baseDir;

    private final Object lock = new Object();

    private int currentDay;
    private int dayEpoch;

    private DataOutputStream currentDayOut;

    private final Map<Integer, List<SaleEvent>> series;
    private final Map<Integer, Map<String, DayProductAgg>> dayAggCache;

    private final Set<String> soldProductsToday;
    private String lastProductToday;
    private int currentRun;
    private String maxRunProduct;
    private int maxRunLength;

    private void logIOException(String where, IOException e) {
        System.err.println(where + ": " + e.getMessage());
        e.printStackTrace(System.err);
    }

    private static final class DayProductAgg {
        int quantity;
        double volume;
        double maxPrice;
        boolean hasMax;
    }

    private interface RecordConsumer {
        void accept(String productId, int quantity, double price);
    }

    public SalesStore(int maxDays, int maxCached, String basePath) {
        this.maxDays = maxDays;
        this.totalDays = maxDays + 1;
        this.maxCached = maxCached;
        this.baseDir = new File(basePath);
        if (!baseDir.exists()) {
            baseDir.mkdirs();
        }

        this.series = new HashMap<>();
        this.dayAggCache = new HashMap<>();

        this.soldProductsToday = new HashSet<>();
        this.lastProductToday = null;
        this.currentRun = 0;
        this.maxRunProduct = null;
        this.maxRunLength = 0;

        this.dayEpoch = 0;

        int loadedDay = loadState();
        synchronized (lock) {
            this.currentDay = loadedDay;
        }

        ensureDayFileExists(loadedDay);
        rebuildTodayTrackingFromDisk();
        openCurrentDayWriter();

        saveStateValue(loadedDay);
    }

    public int getCurrentDay() {
        synchronized (lock) {
            return currentDay;
        }
    }

    private File getDayFile(int day) {
        return new File(baseDir, "day_" + day + ".bin");
    }

    private File getStateFile() {
        return new File(baseDir, "state.bin");
    }

    private int loadState() {
        File f = getStateFile();
        if (!f.exists()) {
            return 0;
        }
        try (DataInputStream in = new DataInputStream(new FileInputStream(f))) {
            int d = in.readInt();
            if (d < 0) return 0;
            return d % totalDays;
        } catch (IOException e) {
            logIOException("SalesStore.loadState", e);
            return 0;
        }
    }

    private void saveStateValue(int day) {
        File f = getStateFile();
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(f))) {
            out.writeInt(day);
        } catch (IOException e) {
            logIOException("SalesStore.saveStateValue", e);
        }
    }

    private void deleteDayFile(int day) {
        File f = getDayFile(day);
        if (f.exists()) {
            f.delete();
        }
    }

    private void ensureDayFileExists(int day) {
        File f = getDayFile(day);
        if (f.exists()) return;
        try (FileOutputStream out = new FileOutputStream(f, true)) {
        } catch (IOException e) {
            logIOException("SalesStore.ensureDayFileExists", e);
        }
    }

    private void openCurrentDayWriter() {
        synchronized (lock) {
            if (currentDayOut != null) return;
            ensureDayFileExists(currentDay);
            try {
                currentDayOut = new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(getDayFile(currentDay), true), WRITE_BUFFER_SIZE)
                );
            } catch (IOException e) {
                logIOException("SalesStore.openCurrentDayWriter", e);
                currentDayOut = null;
            }
        }
    }

    private void closeCurrentDayWriter() {
        synchronized (lock) {
            if (currentDayOut == null) return;
            try {
                currentDayOut.flush();
                currentDayOut.close();
            } catch (IOException e) {
                logIOException("SalesStore.closeCurrentDayWriter", e);
            } finally {
                currentDayOut = null;
            }
        }
    }

    private void readDayRecords(int day, RecordConsumer consumer) {
        File f = getDayFile(day);
        if (!f.exists()) return;

        try (BufferedInputStream bin = new BufferedInputStream(new FileInputStream(f))) {
            bin.mark(8192);
            DataInputStream in = new DataInputStream(bin);

            boolean oldHeaderDetected = false;

            try {
                String first = in.readUTF();
                if (first.isEmpty()) {
                    oldHeaderDetected = true;
                } else {
                    int q = in.readInt();
                    double p = in.readDouble();
                    consumer.accept(first, q, p);
                }
            } catch (EOFException eof) {
                return;
            } catch (IOException e) {
                logIOException("SalesStore.readDayRecords.first", e);
                return;
            }

            if (oldHeaderDetected) {
                try {
                    bin.reset();
                } catch (IOException e) {
                    logIOException("SalesStore.readDayRecords.reset", e);
                    return;
                }
                in = new DataInputStream(bin);
                try {
                    in.readInt();
                } catch (EOFException eof) {
                    return;
                } catch (IOException e) {
                    logIOException("SalesStore.readDayRecords.skipHeader", e);
                    return;
                }
            }

            while (true) {
                try {
                    String productId = in.readUTF();
                    int quantity = in.readInt();
                    double price = in.readDouble();
                    consumer.accept(productId, quantity, price);
                } catch (EOFException eof) {
                    break;
                }
            }
        } catch (IOException e) {
            logIOException("SalesStore.readDayRecords", e);
        }
    }

    private void rebuildTodayTrackingFromDisk() {
        final int day;
        synchronized (lock) {
            day = currentDay;
            soldProductsToday.clear();
            lastProductToday = null;
            currentRun = 0;
            maxRunProduct = null;
            maxRunLength = 0;
        }

        readDayRecords(day, new RecordConsumer() {
            @Override
            public void accept(String productId, int quantity, double price) {
                synchronized (lock) {
                    soldProductsToday.add(productId);
                    if (productId.equals(lastProductToday)) {
                        currentRun++;
                    } else {
                        lastProductToday = productId;
                        currentRun = 1;
                    }
                    if (currentRun > maxRunLength) {
                        maxRunLength = currentRun;
                        maxRunProduct = lastProductToday;
                    }
                }
            }
        });
    }

    private static boolean isInvalidProductId(String productId) {
        return productId == null || productId.isEmpty();
    }

    private static boolean isInvalidPrice(double price) {
        return price < 0.0 || Double.isNaN(price) || Double.isInfinite(price);
    }

    public void nextDay() {
        synchronized (lock) {
            closeCurrentDayWriter();

            int oldDay = currentDay;
            int newDay = (oldDay + 1) % totalDays;

            series.remove(newDay);
            dayAggCache.remove(newDay);

            deleteDayFile(newDay);
            ensureDayFileExists(newDay);

            currentDay = newDay;
            dayEpoch++;

            soldProductsToday.clear();
            lastProductToday = null;
            currentRun = 0;
            maxRunProduct = null;
            maxRunLength = 0;

            try {
                currentDayOut = new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(getDayFile(currentDay), true), WRITE_BUFFER_SIZE)
                );
            } catch (IOException e) {
                logIOException("SalesStore.nextDay.openWriter", e);
                currentDayOut = null;
            }

            saveStateValue(currentDay);

            evictIfNeededUnlocked();

            lock.notifyAll();
        }
    }

    public void addSale(String productId, int quantity, double price) {
        String pid = (productId == null) ? null : productId.trim();
        if (isInvalidProductId(pid)) throw new IllegalArgumentException("Invalid productId");
        if (quantity <= 0) throw new IllegalArgumentException("Invalid quantity");
        if (isInvalidPrice(price)) throw new IllegalArgumentException("Invalid price");

        synchronized (lock) {
            if (currentDayOut == null) {
                openCurrentDayWriter();
            }

            if (currentDayOut == null) {
                throw new IllegalStateException("I/O error");
            }

            try {
                currentDayOut.writeUTF(pid);
                currentDayOut.writeInt(quantity);
                currentDayOut.writeDouble(price);
            } catch (IOException e) {
                logIOException("SalesStore.addSale.write", e);
                closeCurrentDayWriter();
                throw new IllegalStateException("I/O error");
            }

            soldProductsToday.add(pid);
            if (pid.equals(lastProductToday)) {
                currentRun++;
            } else {
                lastProductToday = pid;
                currentRun = 1;
            }
            if (currentRun > maxRunLength) {
                maxRunLength = currentRun;
                maxRunProduct = lastProductToday;
            }

            lock.notifyAll();
        }
    }

    private int distanceFromCurrent(int day) {
        return (currentDay - day + totalDays) % totalDays;
    }

    private int chooseEvictionDayUnlocked() {
        int candidate = -1;
        int maxDist = -1;
        for (Integer d : series.keySet()) {
            int dist = distanceFromCurrent(d);
            if (dist > maxDist) {
                maxDist = dist;
                candidate = d;
            }
        }
        return candidate;
    }

    private void evictIfNeededUnlocked() {
        while (series.size() > maxCached) {
            int evict = chooseEvictionDayUnlocked();
            if (evict == -1) break;
            series.remove(evict);
            dayAggCache.remove(evict);
        }
    }

    private List<SaleEvent> loadDayFromDisk(final int day) {
        final List<SaleEvent> list = new ArrayList<>();
        readDayRecords(day, new RecordConsumer() {
            @Override
            public void accept(String productId, int quantity, double price) {
                list.add(new SaleEvent(productId, quantity, price, day));
            }
        });
        return list;
    }

    private List<SaleEvent> getSeriesMaybeCached(int day) {
        if (day == getCurrentDay()) return null;

        List<SaleEvent> cached;
        boolean canLoad;

        synchronized (lock) {
            cached = series.get(day);
            if (cached != null) return cached;
            canLoad = series.size() < maxCached;
        }

        if (!canLoad) return null;

        List<SaleEvent> loaded = loadDayFromDisk(day);

        synchronized (lock) {
            List<SaleEvent> again = series.get(day);
            if (again != null) return again;
            if (series.size() < maxCached) {
                series.put(day, loaded);
                evictIfNeededUnlocked();
            }
        }

        return loaded;
    }

    private DayProductAgg computeAggFromList(List<SaleEvent> list, String productId) {
        DayProductAgg a = new DayProductAgg();
        for (SaleEvent e : list) {
            if (!productId.equals(e.getProductId())) continue;
            a.quantity += e.getQuantity();
            a.volume += e.getQuantity() * e.getPrice();
            if (!a.hasMax || e.getPrice() > a.maxPrice) {
                a.maxPrice = e.getPrice();
                a.hasMax = true;
            }
        }
        return a;
    }

    private DayProductAgg computeAggFromDisk(final int day, final String productId) {
        final DayProductAgg a = new DayProductAgg();
        readDayRecords(day, new RecordConsumer() {
            @Override
            public void accept(String pid, int quantity, double price) {
                if (!productId.equals(pid)) return;
                a.quantity += quantity;
                a.volume += quantity * price;
                if (!a.hasMax || price > a.maxPrice) {
                    a.maxPrice = price;
                    a.hasMax = true;
                }
            }
        });
        return a;
    }

    private DayProductAgg getDayAgg(int day, String productId) {
        Map<String, DayProductAgg> byProduct;
        DayProductAgg cached;

        synchronized (lock) {
            byProduct = dayAggCache.get(day);
            if (byProduct == null) {
                byProduct = new HashMap<>();
                dayAggCache.put(day, byProduct);
            }
            cached = byProduct.get(productId);
            if (cached != null) return cached;
        }

        List<SaleEvent> list = getSeriesMaybeCached(day);
        DayProductAgg computed = (list != null) ? computeAggFromList(list, productId) : computeAggFromDisk(day, productId);

        synchronized (lock) {
            Map<String, DayProductAgg> again = dayAggCache.get(day);
            if (again == null) {
                again = new HashMap<>();
                dayAggCache.put(day, again);
            }
            DayProductAgg existing = again.get(productId);
            if (existing != null) return existing;
            again.put(productId, computed);
            return computed;
        }
    }

    public double aggregateQuantity(String productId, int lastDays) {
        if (lastDays > maxDays) lastDays = maxDays;
        if (lastDays <= 0) return 0.0;

        String pid = (productId == null) ? null : productId.trim();
        if (isInvalidProductId(pid)) return 0.0;

        int startDay;
        synchronized (lock) {
            startDay = currentDay;
        }

        int day = startDay;
        int total = 0;

        for (int i = 0; i < lastDays; i++) {
            day = (day - 1 + totalDays) % totalDays;
            DayProductAgg a = getDayAgg(day, pid);
            total += a.quantity;
        }

        return total;
    }

    public double aggregateVolume(String productId, int lastDays) {
        if (lastDays > maxDays) lastDays = maxDays;
        if (lastDays <= 0) return 0.0;

        String pid = (productId == null) ? null : productId.trim();
        if (isInvalidProductId(pid)) return 0.0;

        int startDay;
        synchronized (lock) {
            startDay = currentDay;
        }

        int day = startDay;
        double total = 0.0;

        for (int i = 0; i < lastDays; i++) {
            day = (day - 1 + totalDays) % totalDays;
            DayProductAgg a = getDayAgg(day, pid);
            total += a.volume;
        }

        return total;
    }

    public double aggregateAveragePrice(String productId, int lastDays) {
        if (lastDays > maxDays) lastDays = maxDays;
        if (lastDays <= 0) return 0.0;

        String pid = (productId == null) ? null : productId.trim();
        if (isInvalidProductId(pid)) return 0.0;

        int startDay;
        synchronized (lock) {
            startDay = currentDay;
        }

        int day = startDay;
        double totalVolume = 0.0;
        int totalQuantity = 0;

        for (int i = 0; i < lastDays; i++) {
            day = (day - 1 + totalDays) % totalDays;
            DayProductAgg a = getDayAgg(day, pid);
            totalVolume += a.volume;
            totalQuantity += a.quantity;
        }

        if (totalQuantity == 0) return 0.0;
        return totalVolume / totalQuantity;
    }

    public double aggregateMaxPrice(String productId, int lastDays) {
        if (lastDays > maxDays) lastDays = maxDays;
        if (lastDays <= 0) return 0.0;

        String pid = (productId == null) ? null : productId.trim();
        if (isInvalidProductId(pid)) return 0.0;

        int startDay;
        synchronized (lock) {
            startDay = currentDay;
        }

        int day = startDay;
        double max = 0.0;

        for (int i = 0; i < lastDays; i++) {
            day = (day - 1 + totalDays) % totalDays;
            DayProductAgg a = getDayAgg(day, pid);
            if (a.hasMax && a.maxPrice > max) {
                max = a.maxPrice;
            }
        }

        return max;
    }

    private List<SaleEvent> filterEventsFromDisk(final int day, final Set<String> productSet) {
        final List<SaleEvent> result = new ArrayList<>();
        readDayRecords(day, new RecordConsumer() {
            @Override
            public void accept(String productId, int quantity, double price) {
                if (productSet.contains(productId)) {
                    result.add(new SaleEvent(productId, quantity, price, day));
                }
            }
        });
        return result;
    }

    public List<SaleEvent> filterEvents(int daysAgo, List<String> productIds) {
        if (daysAgo < 1 || daysAgo > maxDays) {
            return new ArrayList<>();
        }
        if (productIds == null) {
            return new ArrayList<>();
        }

        int startDay;
        synchronized (lock) {
            startDay = currentDay;
        }

        int day = (startDay - daysAgo + totalDays) % totalDays;

        Set<String> productSet = new HashSet<>();
        for (String p : productIds) {
            if (p == null) continue;
            String t = p.trim();
            if (!t.isEmpty()) productSet.add(t);
        }
        if (productSet.isEmpty()) {
            return new ArrayList<>();
        }

        List<SaleEvent> cached;
        synchronized (lock) {
            cached = series.get(day);
        }

        if (cached != null) {
            List<SaleEvent> result = new ArrayList<>();
            for (SaleEvent e : cached) {
                if (productSet.contains(e.getProductId())) {
                    result.add(e);
                }
            }
            return result;
        }

        return filterEventsFromDisk(day, productSet);
    }

    public boolean waitForSimultaneous(String p1, String p2) throws InterruptedException {
        String a = (p1 == null) ? null : p1.trim();
        String b = (p2 == null) ? null : p2.trim();
        if (isInvalidProductId(a) || isInvalidProductId(b)) throw new IllegalArgumentException("Invalid productId");

        synchronized (lock) {
            int epoch = dayEpoch;
            while (true) {
                if (dayEpoch != epoch) return false;
                if (soldProductsToday.contains(a) && soldProductsToday.contains(b)) {
                    return true;
                }
                lock.wait();
            }
        }
    }

    public String waitForConsecutive(int count) throws InterruptedException {
        if (count <= 0) throw new IllegalArgumentException("Invalid count");

        synchronized (lock) {
            int epoch = dayEpoch;
            while (true) {
                if (dayEpoch != epoch) return null;
                if (maxRunLength >= count) {
                    return maxRunProduct;
                }
                lock.wait();
            }
        }
    }
}
