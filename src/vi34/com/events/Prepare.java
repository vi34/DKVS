package vi34.com.events;

import vi34.com.Connection;
import vi34.com.Operation;

/**
 * Created by vi34 on 12/06/16.
 */
public class Prepare implements Event {
    public static final String TYPE = "Prepare";
    public Request req;
    public int view;
    public int opNum;
    public int commitNum;
    private String s;
    private Connection connection;

    public Prepare(String s, Connection connection) {
        this.connection = connection;
        this.s = s;
        req = new Request();
        String[] tmp = s.split(",");
        String[] msg = tmp[2].split(":");
        req.clientId = Integer.valueOf(msg[0]);
        req.requestNumber = Integer.valueOf(msg[1]);
        String[] reqM = msg[2].split(" ");
        req.op =  Operation.valueOf(reqM[0].toUpperCase());
        req.args = new String[reqM.length - 1];
        System.arraycopy(reqM, 1, req.args, 0, reqM.length - 1);
        view = Integer.valueOf(tmp[1]);
        opNum = Integer.valueOf(tmp[3]);
        commitNum =  Integer.valueOf(tmp[4]);
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
