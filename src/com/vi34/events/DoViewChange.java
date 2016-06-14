package com.vi34.events;

import com.vi34.Connection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by vi34 on 12/06/16.
 */
public class DoViewChange implements Event {
    public static final String TYPE = "DoViewChange";
    public int view;
    public int lastNormal;
    public int opNum;
    public int commitNum;
    public int replicaNum;
    public List<String> log;
    private Connection connection;

    public DoViewChange(String s, Connection connection) {
        this.connection = connection;
        String[] tmp = s.split(",");
        view = Integer.valueOf(tmp[1]);
        lastNormal = Integer.valueOf(tmp[2]);
        opNum = Integer.valueOf(tmp[3]);
        commitNum = Integer.valueOf(tmp[4]);
        replicaNum = Integer.valueOf(tmp[5]);
        if (tmp.length > 6) {
            log = Arrays.asList(tmp[6].split("\\|"));
        } else {
            log = new ArrayList<>();
        }
    }

    public DoViewChange(int view, int lastNormal, int opNum, int commitNum, int replicaNum, List<String> log) {
        this.view = view;
        this.lastNormal = lastNormal;
        this.opNum = opNum;
        this.commitNum = commitNum;
        this.replicaNum = replicaNum;
        this.log = log;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public String toString() {
        return  DoViewChange.TYPE + "," + view + "," + lastNormal + "," + opNum +","+ commitNum + ","
                + replicaNum +","+ String.join("|", log);
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
