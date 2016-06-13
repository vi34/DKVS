package vi34.com.events;

import vi34.com.Connection;

/**
 * Created by vi34 on 12/06/16.
 */
public class Commit implements Event {
    public static final String TYPE = "Commit";
    public int view;
    public int commitNum;
    private String s;
    private Connection connection;

    public Commit(String s, Connection connection) {
        this.connection = connection;
        this.s = s;
        String[] tmp = s.split(",");
        view = Integer.valueOf(tmp[1]);
        commitNum = Integer.valueOf(tmp[2]);
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
