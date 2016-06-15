package com.vi34.events;

import com.vi34.Connection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by vi34 on 12/06/16.
 */
public class RecoveryResponse implements Event {
    public static final String TYPE = "RecoveryResponse";
    public int view;
    public long nonce;
    public int opNum;
    public int commitNum;
    public int replicaNum;
    public List<String> log;
    private Connection connection;

    public RecoveryResponse(String s, Connection connection) {
        this.connection = connection;
        String[] tmp = s.split(",");
        view = Integer.valueOf(tmp[1]);
        nonce = Long.valueOf(tmp[2]);
        opNum = Integer.valueOf(tmp[3]);
        commitNum = Integer.valueOf(tmp[4]);
        replicaNum = Integer.valueOf(tmp[5]);
        if (tmp.length > 6) {
            log = Arrays.asList(tmp[6].split("\\|"));
        } else {
            log = new ArrayList<>();
        }
    }

    public RecoveryResponse(int view, long nonce, int opNum, int commitNum, int replicaNum, List<String> log) {
        this.view = view;
        this.nonce = nonce;
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
        return  RecoveryResponse.TYPE + "," + view + "," + nonce + "," + opNum +","+ commitNum + ","
                + replicaNum +","+ String.join("|", log);
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
