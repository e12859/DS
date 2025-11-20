package sd.common;

public class SaleEvent {
    private final String productId;
    private final int quantity;
    private final double price;
    private final int day;

    public SaleEvent(String productId, int quantity, double price, int day) {
        this.productId = productId;
        this.quantity = quantity;
        this.price = price;
        this.day = day;
    }

    public String getProductId() {
        return productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public double getPrice() {
        return price;
    }

    public int getDay() {
        return day;
    }
}
