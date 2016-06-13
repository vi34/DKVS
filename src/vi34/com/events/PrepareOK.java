package vi34.com.events;

import vi34.com.Connection;
import vi34.com.Operation;

/**
 * Created by vi34 on 12/06/16.
 */
public class PrepareOK implements Event {
    public static final String TYPE = "PrepareOK";
    public int view;
    public int opNum;
    public int replicaNum;
    private String s;
    private Connection connection;

    public PrepareOK(String s, Connection connection) {
        this.connection = connection;
        this.s = s;
        String[] tmp = s.split(",");
        view = Integer.valueOf(tmp[1]);
        opNum = Integer.valueOf(tmp[2]);
        replicaNum = Integer.valueOf(tmp[3]);
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public String toString() {
        return s;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
