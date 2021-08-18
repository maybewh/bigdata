package com.atguigu.flink;

public class LoginEvent {
    private String userId;
    private String ip;
    private String type;

    public LoginEvent(String userId, String ip, String type) {
        this.userId = userId;
        this.ip = ip;
        this.type = type;
    }

    public String getUserId() {
        return userId;
    }
    public String getType() {
        return type;
    }
    public String getIp() {
        return ip;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", type='" + type + '\'' +
                ", ip='" + ip + '\'' +
                '}';
    }
}
