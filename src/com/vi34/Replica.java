package com.vi34;

import com.vi34.events.*;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Created by vi34 on 11/06/16.
 */
public class Replica extends Thread {
    private int replicaNumber;
    private int port;
    private int n;
    private int timeout;
    private int viewNumber;
    private int opNumber;
    private int commitNumber;
    private int startVoteCount;
    private int doViewChangeCount;
    private int lastNormal;
    private Status status;
    private Logger logger;
    private Thread acceptor;
    private Connection[] connections;
    private List<Connection> externalConections = new ArrayList<>();
    private ServerSocket servSocket;
    private Properties config;
    private Map<Integer, ClientInfo> clientsTable = new HashMap<>();
    private Map<String, String> stateMachine = new HashMap<>();
    private Map<Integer, Request> pending = new HashMap<>();
    private Map<Integer, Integer> pendingOk = new HashMap<>();
    private long lastSentTimestamp;
    private DoViewChange best;
    private int bestCommitNum;
    public BlockingQueue<Event> eventQueue;

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
        timeout = Integer.valueOf(config.getProperty("timeout"));
        port = Integer.valueOf(config.getProperty("node." + replicaNumber).split(":")[1]);
        eventQueue = new LinkedBlockingQueue<>();
        status = Status.NORMAL;
    }

    @Override
    public void run() {
        startCommThreads();
        while (!Thread.interrupted()) {
            try {
                Event event = eventQueue.poll(getWaitTime(), TimeUnit.MILLISECONDS);
                if (event == null) {
                    timeExpired();
                    continue;
                }

                System.out.println("Replica "+ replicaNumber + " got: <" + event + ">");
                if (event instanceof Request) {
                        if (status == Status.NORMAL) {
                            processRequest((Request) event);
                        } else {
                            //// TODO: 13/06/16 clarify this case
                            //eventQueue.offer(event);
                        }
                } else if (event instanceof Prepare) {
                    processPrepare((Prepare) event);
                } else if (event instanceof PrepareOK) {
                    processPrepareOK((PrepareOK) event);
                } else if (event instanceof Commit) {
                    processCommit((Commit) event);
                } else if (event instanceof StartViewChange) {
                    processStartViewChange((StartViewChange) event);
                } else if (event instanceof DoViewChange) {
                    processDoViewChange((DoViewChange) event);
                } else if (event instanceof StartView) {
                    processStartView((StartView) event);
                }
            } catch (InterruptedException e) {
                close();
                return;
            }
        }
    }

    private long getWaitTime() {
        if (curPrimary() == replicaNumber) {
            long t = timeout - (System.currentTimeMillis() - lastSentTimestamp) - 100;
            return t > 0 ? t : 0;
        } else {
            return timeout;
        }
    }

    private void timeExpired() {
        if (curPrimary() == replicaNumber) {
            Request msg = new Request(Operation.PING, new String[0], replicaNumber, commitNumber); // FIXME: send commit
            for (int i = 0; i < connections.length; ++i) {
                if (i == replicaNumber - 1)
                    continue;
                send(connections[i], "Replica " + (i+1), msg.toString());
            }
            lastSentTimestamp = System.currentTimeMillis();
        } else {
            startVoteCount = 0;
            startViewChange();
        }
    }

    private void startViewChange() {
        if (status == Status.NORMAL) {
            lastNormal = viewNumber;
        }
        doViewChangeCount = 0;
        best = null;
        bestCommitNum = 0;
        viewNumber++;
        status = Status.VIEW_CHANGE;
        StartViewChange msg = new StartViewChange(viewNumber, replicaNumber);
        for (int i = 0; i < connections.length; ++i) {
            if (i == replicaNumber - 1)
                continue;
            send(connections[i], "Replica " + (i+1), msg.toString());
        }
        lastSentTimestamp = System.currentTimeMillis();
    }

    private void processStartViewChange(StartViewChange startViewChange) {
        if (startViewChange.view > viewNumber) {
            viewNumber = startViewChange.view - 1;
            startVoteCount = 1;
            startViewChange();
        } else if (status == Status.VIEW_CHANGE) {
            if (startViewChange.view == viewNumber) {
                startVoteCount++;
            }
        }

        if (startVoteCount >= n / 2) {
            doViewChange();
        }
    }

    private void processDoViewChange(DoViewChange doViewChange) {
        doViewChangeCount++;
        if (best == null) {
            best = new DoViewChange(viewNumber, lastNormal, opNumber, commitNumber, replicaNumber, getLog());
        }
        if (doViewChange.lastNormal > best.lastNormal || doViewChange.lastNormal == best.lastNormal && doViewChange.opNum > best.opNum) {
            best = doViewChange;
        }
        if (doViewChange.commitNum > bestCommitNum) {
            bestCommitNum = doViewChange.commitNum;
        }

        if (doViewChangeCount >= n / 2) {
            System.out.println("Replica " + (replicaNumber) + " is new leader");
            viewNumber = doViewChange.view;
            status = Status.NORMAL;
            rewriteLog(best.log);
            opNumber = best.opNum;
            startView();
            if (commitNumber < bestCommitNum) {
                for (int i = commitNumber; i <= bestCommitNum; ++i) {
                    pending.put(i, getReqFromLog(best.log.get(i)));
                }
                commit(commitNumber, bestCommitNum);
            }
        }
    }

    private void rewriteLog(List<String> log) {
        Path logFile = Paths.get("dkvs_"+replicaNumber+".log");
        try {
            Files.write(logFile, log, StandardOpenOption.WRITE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void startView() {
        StartView msg = new StartView(viewNumber, opNumber, commitNumber, getLog());
        broadcast(msg.toString());
    }

    private void processCommit(Commit commit) {
        // TODO: 14/06/16 check viewNum
        commit(commitNumber, commit.commitNum);
    }

    private void processPrepareOK(PrepareOK prepareOK) {
        if (prepareOK.opNum <= commitNumber)
            return;
        pendingOk.putIfAbsent(prepareOK.opNum, 1);
        pendingOk.compute(prepareOK.opNum, (k, v) -> v + 1);
        if (pendingOk.get(prepareOK.opNum) >= n / 2) {
            commit(commitNumber, prepareOK.opNum);
            commitInform();
        }
    }

    private void doViewChange() {
        DoViewChange msg = new DoViewChange(viewNumber, lastNormal, opNumber, commitNumber, replicaNumber, getLog());
        if (curPrimary() != replicaNumber) {
            send(connections[curPrimary() - 1], "Replica" + curPrimary(), msg.toString());
        }
    }

    private List<String> getLog() {
        try {
            return Files.readAllLines(Paths.get("dkvs_"+replicaNumber+".log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void commit(int from, int to) {
        for (int i = from + 1; i <= to; ++i) {
            Request request = pending.get(i);
            Response r;
            if (request.op == Operation.SET) {
                stateMachine.put(request.args[0], request.args[1]);
                r = Response.STORED;
            } else {
                stateMachine.remove(request.args[0]);
                r = Response.DELETED;
            }
            Reply reply = new Reply(viewNumber, request.requestNumber, r.toString());
            if (clientsTable.containsKey(request.clientId) && clientsTable.get(request.clientId).lastRequestInd == request.requestNumber) {
                clientsTable.put(request.clientId, new ClientInfo(request.requestNumber,reply.toString()));
            }
            if (curPrimary() == replicaNumber) {
                send(request.getConnection(), "Client ", reply.toString());
            }
            pending.remove(i);
        }
        commitNumber = to;
    }

    private void commitInform() {
        Commit msg = new Commit(viewNumber, commitNumber);
        broadcast(msg.toString());
    }

    private void processStartView(StartView startView) {
        rewriteLog(startView.log);
        opNumber = startView.opNum;
        viewNumber = startView.view;
        status = Status.NORMAL;
        if (commitNumber < startView.commitNum) {
            for (int i = commitNumber; i <= startView.commitNum; ++i) {
                pending.put(i, getReqFromLog(startView.log.get(i)));
            }
            commit(commitNumber, startView.commitNum);
        }
        if (opNumber > commitNumber) {
            PrepareOK msg = new PrepareOK(viewNumber, opNumber, commitNumber);
            send(connections[curPrimary() - 1], "leader", msg.toString());
        }
    }

    private Request getReqFromLog(String logEntry) {
        String[] tmp = logEntry.split("]:")[1].split(" ");
        Operation op  =  Operation.valueOf(tmp[0].toUpperCase());
        String[] args = new String[tmp.length - 1];
        System.arraycopy(tmp, 1, tmp, 0, tmp.length - 1);
        return new Request(op, args, -1, -1);
    }

    private void processRequest(Request request) {
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
            Reply reply = new Reply(viewNumber, request.requestNumber, response);
            send(request.getConnection(), "Client " + request.clientId, reply.toString());
        } else if (request.op == Operation.PING) {
            Reply reply = new Reply(viewNumber, request.requestNumber, Response.PONG.toString());
            send(request.getConnection(), ""+request.clientId, reply.toString());
        } else {
            if (curPrimary() != replicaNumber) {
                System.out.println("WARNING: Not leader got request\nprimary: " + curPrimary() + " replica: " + replicaNumber);
            } else {
                opNumber++;
                logger.info(request.op + " " + String.join(" ", request.args));
                clientsTable.put(request.clientId, new ClientInfo(request.requestNumber, null));
                prepare(request);
                pending.put(opNumber, request);
            }
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

    private void processPrepare(Prepare prepare) {
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
        PrepareOK msg = new PrepareOK(viewNumber, prepare.opNum, replicaNumber);
        send(connections[curPrimary() - 1], "Leader " + (curPrimary()), msg.toString());
    }

    private void send(Connection connection, String to, String msg) {
        connection.messageQueue.offer(msg);
        System.out.println("Replica " + replicaNumber + " ---> " + to + " : <" + msg + ">");
    }

    private void prepare(Request request) {
        Prepare prepareMsg = new Prepare(request, viewNumber, opNumber, commitNumber);
        broadcast(prepareMsg.toString());
    }

    private void broadcast(String msg) {
        for (int i = 0; i < connections.length; ++i) {
            if (i == replicaNumber - 1)
                continue;
            send(connections[i], "Replica " + (i+1), msg);
        }
        lastSentTimestamp = System.currentTimeMillis();
    }

    private int curPrimary() {
        return viewNumber % n + 1;
    }

    private void startCommThreads() {
        acceptor  = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)){
                servSocket = serverSocket;
                while (!Thread.interrupted()) {
                    Connection conn = new Connection(serverSocket.accept(), this);
                    conn.messageQueue.offer(new Reply(viewNumber, -1, "ACCEPT").toString());
                    externalConections.add(conn);
                }
            } catch (IOException ignored) {}
        });
        acceptor.start();
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

    private void close() {
        for (Connection connection : connections) {
            if (connection == null)
                continue;
            connection.close();
        }
        try {
            servSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        acceptor.interrupt();
        externalConections.forEach(Connection::close);
    }
}
