package sd.server;

import sd.common.ProtocolConstants;
import sd.common.SaleEvent;

import java.io.DataInputStream;
import java.io.DataOutputStream;
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
    private final int maxDays;
    private final int maxCached;
    private final File baseDir;
    private int currentDay;
    private final Map<Integer, List<SaleEvent>> series;
    private final Map<String, Double> aggregateCache;

    public SalesStore(int maxDays, int maxCached, String basePath) {
        this.maxDays = maxDays;
        this.maxCached = maxCached;
        this.baseDir = new File(basePath);
        if (!baseDir.exists()) {
            baseDir.mkdirs();
        }
        this.series = new HashMap<>();
        this.aggregateCache = new HashMap<>();
        this.currentDay = loadState();
        List<SaleEvent> currentList = loadDayFromDisk(this.currentDay);
        series.put(this.currentDay, currentList);
        saveState();
    }

    public synchronized int getCurrentDay() {
        return currentDay;
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
            if (d < 0 || d >= maxDays) {
                return 0;
            }
            return d;
        } catch (IOException e) {
            return 0;
        }
    }

    private void saveState() {
        File f = getStateFile();
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(f))) {
            out.writeInt(currentDay);
        } catch (IOException e) {
        }
    }

    private void persistDay(int day) {
        List<SaleEvent> list = series.get(day);
        File f = getDayFile(day);
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(f))) {
            if (list == null) {
                out.writeInt(0);
                return;
            }
            out.writeInt(list.size());
            for (SaleEvent e : list) {
                out.writeUTF(e.getProductId());
                out.writeInt(e.getQuantity());
                out.writeDouble(e.getPrice());
            }
        } catch (IOException e) {
        }
    }

    private List<SaleEvent> loadDayFromDisk(int day) {
        File f = getDayFile(day);
        if (!f.exists()) {
            return new ArrayList<>();
        }
        List<SaleEvent> list = new ArrayList<>();
        try (DataInputStream in = new DataInputStream(new FileInputStream(f))) {
            int n = in.readInt();
            for (int i = 0; i < n; i++) {
                String productId = in.readUTF();
                int quantity = in.readInt();
                double price = in.readDouble();
                list.add(new SaleEvent(productId, quantity, price, day));
            }
        } catch (IOException e) {
        }
        return list;
    }

    private int countCachedPastSeries() {
        int count = 0;
        for (Integer d : series.keySet()) {
            if (d != currentDay) {
                count++;
            }
        }
        return count;
    }

    private int chooseEvictionDay() {
        int candidate = -1;
        for (Integer d : series.keySet()) {
            if (d == currentDay) {
                continue;
            }
            if (candidate == -1) {
                candidate = d;
            } else {
                int diffCand = (currentDay - candidate + maxDays) % maxDays;
                int diffD = (currentDay - d + maxDays) % maxDays;
                if (diffD > diffCand) {
                    candidate = d;
                }
            }
        }
        return candidate;
    }

    private String cacheKey(int aggType, String productId, int lastDays) {
        return aggType + "|" + productId + "|" + lastDays;
    }

    public synchronized void nextDay() {
        persistDay(currentDay);
        int newDay = (currentDay + 1) % maxDays;
        series.remove(newDay);
        File f = getDayFile(newDay);
        if (f.exists()) {
            f.delete();
        }
        currentDay = newDay;
        series.put(currentDay, new ArrayList<>());
        while (countCachedPastSeries() > maxCached) {
            int evict = chooseEvictionDay();
            if (evict == -1) {
                break;
            }
            series.remove(evict);
        }
        aggregateCache.clear();
        saveState();
        notifyAll();
    }

    public synchronized void addSale(String productId, int quantity, double price) {
        List<SaleEvent> list = series.get(currentDay);
        if (list == null) {
            list = new ArrayList<>();
            series.put(currentDay, list);
        }
        SaleEvent event = new SaleEvent(productId, quantity, price, currentDay);
        list.add(event);
        aggregateCache.clear();
        notifyAll();
    }

    private List<SaleEvent> getSeriesMaybeCached(int day) {
        List<SaleEvent> list = series.get(day);
        if (list != null) {
            return list;
        }
        if (day == currentDay) {
            list = loadDayFromDisk(day);
            series.put(day, list);
            return list;
        }
        if (countCachedPastSeries() < maxCached) {
            List<SaleEvent> loaded = loadDayFromDisk(day);
            series.put(day, loaded);
            return loaded;
        }
        return null;
    }

    private int quantityFromList(List<SaleEvent> list, String productId) {
        int total = 0;
        for (SaleEvent e : list) {
            if (productId.equals(e.getProductId())) {
                total += e.getQuantity();
            }
        }
        return total;
    }

    private int quantityFromDisk(String productId, int day) {
        File f = getDayFile(day);
        if (!f.exists()) {
            return 0;
        }
        int total = 0;
        try (DataInputStream in = new DataInputStream(new FileInputStream(f))) {
            int n = in.readInt();
            for (int i = 0; i < n; i++) {
                String id = in.readUTF();
                int q = in.readInt();
                double price = in.readDouble();
                if (productId.equals(id)) {
                    total += q;
                }
            }
        } catch (IOException e) {
        }
        return total;
    }

    public synchronized double aggregateQuantity(String productId, int lastDays) {
        if (lastDays > maxDays) {
            lastDays = maxDays;
        }
        String key = cacheKey(ProtocolConstants.AGG_QUANTITY, productId, lastDays);
        Double cached = aggregateCache.get(key);
        if (cached != null) {
            return cached;
        }
        int day = currentDay;
        int counted = 0;
        int total = 0;
        while (counted < lastDays) {
            day = (day - 1 + maxDays) % maxDays;
            List<SaleEvent> list = getSeriesMaybeCached(day);
            if (list != null) {
                total += quantityFromList(list, productId);
            } else {
                total += quantityFromDisk(productId, day);
            }
            counted++;
        }
        double result = total;
        aggregateCache.put(key, result);
        return result;
    }

    private double volumeFromList(List<SaleEvent> list, String productId) {
        double total = 0.0;
        for (SaleEvent e : list) {
            if (productId.equals(e.getProductId())) {
                total += e.getQuantity() * e.getPrice();
            }
        }
        return total;
    }

    private double volumeFromDisk(String productId, int day) {
        File f = getDayFile(day);
        if (!f.exists()) {
            return 0.0;
        }
        double total = 0.0;
        try (DataInputStream in = new DataInputStream(new FileInputStream(f))) {
            int n = in.readInt();
            for (int i = 0; i < n; i++) {
                String id = in.readUTF();
                int q = in.readInt();
                double price = in.readDouble();
                if (productId.equals(id)) {
                    total += q * price;
                }
            }
        } catch (IOException e) {
        }
        return total;
    }

    public synchronized double aggregateVolume(String productId, int lastDays) {
        if (lastDays > maxDays) {
            lastDays = maxDays;
        }
        String key = cacheKey(ProtocolConstants.AGG_VOLUME, productId, lastDays);
        Double cached = aggregateCache.get(key);
        if (cached != null) {
            return cached;
        }
        int day = currentDay;
        int counted = 0;
        double total = 0.0;
        while (counted < lastDays) {
            day = (day - 1 + maxDays) % maxDays;
            List<SaleEvent> list = getSeriesMaybeCached(day);
            if (list != null) {
                total += volumeFromList(list, productId);
            } else {
                total += volumeFromDisk(productId, day);
            }
            counted++;
        }
        aggregateCache.put(key, total);
        return total;
    }

    private double[] avgFromList(List<SaleEvent> list, String productId) {
        double volume = 0.0;
        int quantity = 0;
        for (SaleEvent e : list) {
            if (productId.equals(e.getProductId())) {
                volume += e.getQuantity() * e.getPrice();
                quantity += e.getQuantity();
            }
        }
        return new double[]{volume, quantity};
    }

    private double[] avgFromDisk(String productId, int day) {
        File f = getDayFile(day);
        if (!f.exists()) {
            return new double[]{0.0, 0.0};
        }
        double volume = 0.0;
        int quantity = 0;
        try (DataInputStream in = new DataInputStream(new FileInputStream(f))) {
            int n = in.readInt();
            for (int i = 0; i < n; i++) {
                String id = in.readUTF();
                int q = in.readInt();
                double price = in.readDouble();
                if (productId.equals(id)) {
                    volume += q * price;
                    quantity += q;
                }
            }
        } catch (IOException e) {
        }
        return new double[]{volume, quantity};
    }

    public synchronized double aggregateAveragePrice(String productId, int lastDays) {
        if (lastDays > maxDays) {
            lastDays = maxDays;
        }
        String key = cacheKey(ProtocolConstants.AGG_AVG_PRICE, productId, lastDays);
        Double cached = aggregateCache.get(key);
        if (cached != null) {
            return cached;
        }
        int day = currentDay;
        int counted = 0;
        double volume = 0.0;
        int quantity = 0;
        while (counted < lastDays) {
            day = (day - 1 + maxDays) % maxDays;
            List<SaleEvent> list = getSeriesMaybeCached(day);
            if (list != null) {
                double[] tmp = avgFromList(list, productId);
                volume += tmp[0];
                quantity += (int) tmp[1];
            } else {
                double[] tmp = avgFromDisk(productId, day);
                volume += tmp[0];
                quantity += (int) tmp[1];
            }
            counted++;
        }
        if (quantity == 0) {
            aggregateCache.put(key, 0.0);
            return 0.0;
        }
        double result = volume / quantity;
        aggregateCache.put(key, result);
        return result;
    }

    private double maxFromList(List<SaleEvent> list, String productId) {
        double max = 0.0;
        boolean found = false;
        for (SaleEvent e : list) {
            if (productId.equals(e.getProductId())) {
                if (!found || e.getPrice() > max) {
                    max = e.getPrice();
                    found = true;
                }
            }
        }
        if (!found) {
            return 0.0;
        }
        return max;
    }

    private double maxFromDisk(String productId, int day) {
        File f = getDayFile(day);
        if (!f.exists()) {
            return 0.0;
        }
        double max = 0.0;
        boolean found = false;
        try (DataInputStream in = new DataInputStream(new FileInputStream(f))) {
            int n = in.readInt();
            for (int i = 0; i < n; i++) {
                String id = in.readUTF();
                int q = in.readInt();
                double price = in.readDouble();
                if (productId.equals(id)) {
                    if (!found || price > max) {
                        max = price;
                        found = true;
                    }
                }
            }
        } catch (IOException e) {
        }
        if (!found) {
            return 0.0;
        }
        return max;
    }

    public synchronized double aggregateMaxPrice(String productId, int lastDays) {
        if (lastDays > maxDays) {
            lastDays = maxDays;
        }
        String key = cacheKey(ProtocolConstants.AGG_MAX_PRICE, productId, lastDays);
        Double cached = aggregateCache.get(key);
        if (cached != null) {
            return cached;
        }
        int day = currentDay;
        int counted = 0;
        double max = 0.0;
        boolean found = false;
        while (counted < lastDays) {
            day = (day - 1 + maxDays) % maxDays;
            List<SaleEvent> list = getSeriesMaybeCached(day);
            double v;
            if (list != null) {
                v = maxFromList(list, productId);
            } else {
                v = maxFromDisk(productId, day);
            }
            if (v > 0.0) {
                if (!found || v > max) {
                    max = v;
                    found = true;
                }
            }
            counted++;
        }
        double result;
        if (!found) {
            result = 0.0;
        } else {
            result = max;
        }
        aggregateCache.put(key, result);
        return result;
    }

    public synchronized List<SaleEvent> filterEvents(int day, List<String> productIds) {
        Set<String> set = new HashSet<>(productIds);
        List<SaleEvent> list = getSeriesMaybeCached(day);
        List<SaleEvent> result = new ArrayList<>();
        if (list != null) {
            for (SaleEvent e : list) {
                if (set.contains(e.getProductId())) {
                    result.add(e);
                }
            }
            return result;
        }
        File f = getDayFile(day);
        if (!f.exists()) {
            return result;
        }
        try (DataInputStream in = new DataInputStream(new FileInputStream(f))) {
            int n = in.readInt();
            for (int i = 0; i < n; i++) {
                String productId = in.readUTF();
                int quantity = in.readInt();
                double price = in.readDouble();
                if (set.contains(productId)) {
                    result.add(new SaleEvent(productId, quantity, price, day));
                }
            }
        } catch (IOException e) {
        }
        return result;
    }

    public synchronized boolean waitForSimultaneous(String product1, String product2) throws InterruptedException {
        int startDay = currentDay;
        while (true) {
            if (currentDay != startDay) {
                return false;
            }
            if (hasBothProductsInCurrentDay(product1, product2)) {
                return true;
            }
            wait();
        }
    }

    private boolean hasBothProductsInCurrentDay(String product1, String product2) {
        List<SaleEvent> list = series.get(currentDay);
        if (list == null) {
            return false;
        }
        boolean seen1 = false;
        boolean seen2 = false;
        for (SaleEvent e : list) {
            String p = e.getProductId();
            if (product1.equals(p)) {
                seen1 = true;
            }
            if (product2.equals(p)) {
                seen2 = true;
            }
            if (seen1 && seen2) {
                return true;
            }
        }
        return false;
    }

    public synchronized String waitForConsecutive(int count) throws InterruptedException {
        int startDay = currentDay;
        while (true) {
            if (currentDay != startDay) {
                return null;
            }
            String product = findProductWithConsecutiveInCurrentDay(count);
            if (product != null) {
                return product;
            }
            wait();
        }
    }

    private String findProductWithConsecutiveInCurrentDay(int count) {
        List<SaleEvent> list = series.get(currentDay);
        if (list == null) {
            return null;
        }
        String currentProduct = null;
        int run = 0;
        for (SaleEvent e : list) {
            String p = e.getProductId();
            if (!p.equals(currentProduct)) {
                currentProduct = p;
                run = 1;
            } else {
                run++;
            }
            if (run >= count) {
                return currentProduct;
            }
        }
        return null;
    }
}
