package apitest.pattern.beans;

public class User {

    private String userId;

    private String orderId;

    private String behave;

    public User() {
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getBehave() {
        return behave;
    }

    public void setBehave(String behave) {
        this.behave = behave;
    }
}
