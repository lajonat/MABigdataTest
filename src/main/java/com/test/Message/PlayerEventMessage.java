package com.test.Message;

public class PlayerEventMessage {
    private String user_id;
    private String event;
    private String time;
    private String ip;
    private String params;

    public String getUser_id() {
        return user_id;
    }

    public String getEvent() {
        return event;
    }

    public String getTime() {
        return time;
    }

    public String getIp() {
        return ip;
    }

    public String getParams() {
        return params;
    }

    public PlayerEventMessage() {
    }

    /**
     * Make sure the record is valid
     * @return true if all fields exist, false otherwise
     */
    public boolean isValid(){
        return
                ((user_id != null) &&
                        (event != null) &&
                        (time != null) &&
                        (ip != null) &&
                        (params != null));
    }
}
