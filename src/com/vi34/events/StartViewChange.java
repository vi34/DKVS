package com.vi34.events;

import com.vi34.Connection;

/**
 * Created by vi34 on 12/06/16.
 */
public class StartViewChange implements Event {
    public static final String TYPE = "StartViewChange";
    public int view;
    public int replicaNum;
    private Connection connection;

    public StartViewChange(String s, Connection connection) {
        this.connection = connection;
        String[] tmp = s.split(",");
        view = Integer.valueOf(tmp[1]);
        replicaNum = Integer.valueOf(tmp[2]);
    }

    public StartViewChange(int view, int replicaNum) {
        this.view = view;
        this.replicaNum = replicaNum;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public String toString() {
        return  StartViewChange.TYPE + "," + view + "," + replicaNum;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
