package vi34.com;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by vi34 on 11/06/16.
 */
public class State {
    int replicaNumber;
    int viewNumber;
    int opNumber;
    int commitNumber;
    String[] config;
    Status status;
    File log;
    List<ClientInfo> clientsTable = new ArrayList<>();

    class ClientInfo {
        int lastRequestInd;
        String response;
    }
}
