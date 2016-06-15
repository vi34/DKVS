package com.vi34.events;

import com.vi34.Connection;

/**
 * Created by vi34 on 12/06/16.
 */
public class Recovery implements Event {
    public static final String TYPE = "Recovery";
    public int replicaNum;
    public long nonce;
    private Connection connection;

    public Recovery(String s, Connection connection) {
        this.connection = connection;
        String[] tmp = s.split(",");
        replicaNum = Integer.valueOf(tmp[1]);
        nonce = Long.valueOf(tmp[2]);
    }

    public Recovery(int replicaNum, long nonce) {
        this.replicaNum = replicaNum;
        this.nonce = nonce;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public String toString() {
        return  Recovery.TYPE + "," + replicaNum + "," + nonce;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
