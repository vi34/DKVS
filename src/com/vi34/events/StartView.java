package com.vi34.events;

import com.vi34.Connection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by vi34 on 12/06/16.
 */
public class StartView implements Event {
    public static final String TYPE = "StartView";
    public int view;
    public int opNum;
    public int commitNum;
    public List<String> log;
    private Connection connection;

    public StartView(String s, Connection connection) {
        this.connection = connection;
        String[] tmp = s.split(",");
        view = Integer.valueOf(tmp[1]);
        opNum = Integer.valueOf(tmp[2]);
        commitNum = Integer.valueOf(tmp[3]);
        if (tmp.length > 4) {
            log = Arrays.asList(tmp[4].split("\\|"));
        } else {
            log = new ArrayList<>();
        }
    }

    public StartView(int view, int opNum, int commitNum, List<String> log) {
        this.view = view;
        this.opNum = opNum;
        this.commitNum = commitNum;
        this.log = log;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public String toString() {
        return  StartView.TYPE + "," + view + "," + opNum +","+ commitNum + "," + String.join("|", log);
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
