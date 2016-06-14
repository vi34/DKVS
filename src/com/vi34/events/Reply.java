package com.vi34.events;

import com.vi34.Connection;
import com.vi34.Response;

/**
 * Created by vi34 on 12/06/16.
 */
public class Reply implements Event {
    public static final String TYPE = "Reply";
    public int view;
    public int requestNum;
    public String response;
    private Connection connection;

    public Reply(String s, Connection connection) {
        this.connection = connection;
        String[] tmp = s.split(",");
        view = Integer.valueOf(tmp[1]);
        requestNum = Integer.valueOf(tmp[2]);
        response = tmp[3];
    }

    public Reply(int view, int requestNum, String response) {
        this.view = view;
        this.requestNum = requestNum;
        this.response = response;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public String toString() {
        return  Reply.TYPE + "," + view + "," + requestNum + "," + response;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
