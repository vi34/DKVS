package vi34.com;

import vi34.com.events.*;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Created by vi34 on 11/06/16.
 */
public class Replica implements Runnable{
    int replicaNumber;
    int port;
    int n;
    int viewNumber;
    int opNumber;
    int commitNumber;
    Status status;
    Logger logger;
    Connection[] connections;
    Properties config;
    BlockingQueue<Event> eventQueue;
    Map<Integer, ClientInfo> clientsTable = new HashMap<>();
    Map<String, String> stateMachine = new HashMap<>();
    Map<Integer, Request> pending = new HashMap<>();
    Map<Integer, Integer> pendingOk = new HashMap<>();

    class ClientInfo {
        int lastRequestInd;
        String response;

        public ClientInfo(int lastRequestInd, String response) {
            this.lastRequestInd = lastRequestInd;
            this.response = response;
        }
    }

    Replica(int idx) {
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tH:%1$tM:%1$tS]: %5$s %n");
        replicaNumber = idx;
        config = new Properties();
        try (FileInputStream inputStream = new FileInputStream("dkvs.properties")) {
            config.load(inputStream);
            logger = Logger.getLogger("Replica_" + replicaNumber);
            FileHandler fh = new FileHandler("dkvs_"+replicaNumber+".log");
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);
            logger.setUseParentHandlers(false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        n = Integer.valueOf(config.getProperty("n"));
        port = Integer.valueOf(config.getProperty("node." + replicaNumber).split(":")[1]);
        eventQueue = new LinkedBlockingQueue<>();
        status = Status.NORMAL;
    }

    @Override
    public void run() {
        startCommThreads();
        while (!Thread.interrupted()) {
            try {
                Event event = eventQueue.take();
                System.out.println("Replica "+ replicaNumber + " got: <" + event + ">");
                if (event instanceof Request) {
                    if (curPrimary() != replicaNumber) {
                        System.out.println("WARNING: Got request\nprimary: " + curPrimary() + " replica: " + replicaNumber);
                    } else {
                        if (status == Status.NORMAL) {
                            processRequest((Request) event);
                        } else {
                            //// TODO: 13/06/16 clarify this case
                            eventQueue.offer(event);
                        }
                    }
                } else if (event instanceof Prepare) {
                    processPrepare((Prepare) event);
                } else if (event instanceof PrepareOK) {
                    processPrepareOK((PrepareOK) event);
                } else if (event instanceof Commit) {
                    processCommit((Commit) event);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();

            }
        }
    }

    void processCommit(Commit commit) {
        // TODO: 14/06/16 check viewNum
        commit(commitNumber, commit.commitNum);
    }

    void processPrepareOK(PrepareOK prepareOK) {
        if (prepareOK.opNum <= commitNumber)
            return;
        pendingOk.putIfAbsent(prepareOK.opNum, 1);
        pendingOk.compute(prepareOK.opNum, (k, v) -> v + 1);
        if (pendingOk.get(prepareOK.opNum) >= n / 2) {
            commit(commitNumber, prepareOK.opNum);
            commitInform();
        }
    }

    void commit(int from, int to) {
        for (int i = from + 1; i <= to; ++i) {
            Request request = pending.get(i);
            String reply = "Reply," + viewNumber + "," + request.requestNumber + ",";
            if (request.op == Operation.SET) {
                stateMachine.put(request.args[0], request.args[1]);
                reply += Response.STORED;
            } else if (request.op == Operation.DELETE) {
                stateMachine.remove(request.args[0]);
                reply += Response.DELETED;
            }
            if (clientsTable.get(request.clientId).lastRequestInd == request.requestNumber) {
                clientsTable.put(request.clientId, new ClientInfo(request.requestNumber,reply));
            }
            if (curPrimary() == replicaNumber) {
                send(request.getConnection(), "Client ", reply);
            }
            pending.remove(i);
        }
        commitNumber = to;
    }

    void commitInform() {
        for (int i = 0; i < connections.length; ++i) {
            Connection connection = connections[i];
            if (connection == null)
                continue;
            String msg = Commit.TYPE + "," + viewNumber + "," + commitNumber;
            send(connection, "Replica " + (i+1), msg);
        }
    }

    void processRequest(Request request) {
        if (clientsTable.containsKey(request.clientId)) {
            ClientInfo info = clientsTable.get(request.clientId);
            if (info.lastRequestInd == request.requestNumber && info.response != null) {
                send(request.getConnection(), "Client " + request.clientId, info.response);
                return;
            } else if (info.lastRequestInd > request.requestNumber) {
                //drop
                return;
            }
        }

        if (request.op == Operation.GET) {
            String response = evaluate(request.args[0]);
            send(request.getConnection(), "Client " + request.clientId, response);
        } else if (request.op == Operation.PING) {
            send(request.getConnection(), "Client " + request.clientId, Response.PONG.toString());
        } else {
            opNumber++;
            logger.info(request.op + " " + String.join(" ", request.args));
            clientsTable.put(request.clientId, new ClientInfo(request.requestNumber, null));
            prepare(request);
            pending.put(opNumber, request);
        }

    }

    private String evaluate(String key) {
        String response = stateMachine.get(key);
        if (response == null) {
            response = Response.NOT_FOUND.toString();
        } else {
            response = Response.VALUE.toString() + " " + key + " " + response;
        }
        return response;
    }

    void processPrepare(Prepare prepare) {
        if (prepare.opNum > opNumber) {
            // todo: state transfer
            opNumber = prepare.opNum - 1;
        }
        opNumber++; // TODO: 13/06/16 clarify
        logger.info(prepare.req.op + " " + String.join(" ", prepare.req.args));
        clientsTable.put(prepare.req.clientId, new ClientInfo(prepare.req.requestNumber, null));
        prepareOk(prepare);
        pending.put(prepare.opNum, prepare.req);
    }

    private void prepareOk(Prepare prepare) {
        String msg = PrepareOK.TYPE + "," + viewNumber + "," + prepare.opNum + "," + replicaNumber;
        send(connections[curPrimary() - 1], "Leader " + (curPrimary()), msg);
    }

    private void send(Connection connection, String to, String msg) {
        connection.messageQueue.offer(msg);
        System.out.println("Replica " + replicaNumber + " ---> " + to + " : <" + msg + ">");
    }

    private void prepare(Request request) {
        String prepareMsg = Prepare.TYPE + "," + viewNumber + ","
                            + request.clientId + ":" + request.requestNumber + ":" + request.op + " " + String.join(" ", request.args)
                            + "," + opNumber + "," + commitNumber;
        for (int i = 0; i < connections.length; ++i) {
            if (i == replicaNumber - 1)
                continue;
            send(connections[i], "Replica " + (i+1), prepareMsg);
        }
    }

    private int curPrimary() {
        return viewNumber % n + 1;
    }

    private void startCommThreads() {
        Thread accept  = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)){
                while (!Thread.interrupted()) {
                    new Connection(serverSocket.accept(), this);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        accept.start();
        connections = new Connection[n];

        for (int i = 0; i < n; ++i) {
            if (i == replicaNumber - 1)
                continue;
            String[] address = config.getProperty("node." + (i + 1)).split(":");
            try {
                connections[i] = new Connection(new Socket(address[0], Integer.valueOf(address[1])), this);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
