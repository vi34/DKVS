package com.vi34.events;

import com.vi34.Connection;

/**
 * Created by vi34 on 12/06/16.
 */
public class Commit implements Event {
    public static final String TYPE = "Commit";
    public int view;
    public int commitNum;
    private Connection connection;

    public Commit(String s, Connection connection) {
        this.connection = connection;
        String[] tmp = s.split(",");
        view = Integer.valueOf(tmp[1]);
        commitNum = Integer.valueOf(tmp[2]);
    }

    public Commit(int view, int commitNum) {
        this.view = view;
        this.commitNum = commitNum;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public String toString() {
        return  Commit.TYPE + "," + view + "," + commitNum;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
