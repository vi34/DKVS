package com.vi34;

import com.vi34.events.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by vi34 on 11/06/16.
 */
public class Replica extends Thread {
    public int replicaNumber;
    private int port;
    private int n;
    public int timeout;
    private int viewNumber;
    private int opNumber;
    private int commitNumber;
    private int startVoteCount;
    private int doViewChangeCount;
    private int lastNormal;
    private long nonce;
    private int recoveryCount;
    private Status status;
    private MyLogger logger;
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
    private RecoveryResponse leadRecovery;
    private int bestCommitNum;
    public BlockingQueue<Event> eventQueue;
    private String logPath;

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
        logPath = "logs/dkvs_"+replicaNumber+".log";
        try (FileInputStream inputStream = new FileInputStream("dkvs.properties")) {
            config.load(inputStream);
            if (Files.exists(Paths.get(logPath))) {
                status = Status.RECOVERING;
            } else {
                status = Status.NORMAL;
            }
            logger = new MyLogger(Paths.get(logPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        n = Integer.valueOf(config.getProperty("n"));
        timeout = Integer.valueOf(config.getProperty("timeout"));
        port = Integer.valueOf(config.getProperty("node." + replicaNumber).split(":")[1]);
        eventQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void run() {
        startCommThreads();
        if (status == Status.RECOVERING){
            startRecovery();
        }
        while (!Thread.interrupted()) {
            try {
                Event event = eventQueue.poll(getWaitTime(), TimeUnit.MILLISECONDS);
                if (event == null) {
                    timeExpired();
                    continue;
                }

                System.out.println("Replica "+ replicaNumber + " got: <" + event + ">");
                if (status == Status.RECOVERING) {
                    if (event instanceof RecoveryResponse) {
                        processRecoveryResponse((RecoveryResponse) event);
                    }
                } else if (event instanceof Recovery) {
                        processRecovery((Recovery) event);
                } else if (event instanceof Request) {
                    processRequest((Request) event);
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
                } else if (event instanceof Reply) {
                    processReply((Reply) event);
                }
            } catch (InterruptedException e) {
                close();
                return;
            }
        }
        close();
    }

    private void processRecoveryResponse(RecoveryResponse recoveryResponse) {
        if (recoveryResponse.nonce == nonce) {
            recoveryCount++;
            if (recoveryResponse.view % n + 1 == recoveryResponse.replicaNum ) {
                leadRecovery = recoveryResponse;
            }
        }
        if (recoveryCount >= n / 2 && leadRecovery != null) {
            viewNumber = leadRecovery.view;
            rewriteLog(leadRecovery.log);
            opNumber = leadRecovery.opNum;
            if (commitNumber < leadRecovery.commitNum) {
                for (int i = commitNumber + 1; i <= leadRecovery.commitNum; ++i) {
                    pending.put(i, getReqFromLog(leadRecovery.log.get(i - 1)));
                }
                commit(commitNumber, leadRecovery.commitNum);
            }
            status = Status.NORMAL;
        }
    }

    private void processRecovery(Recovery recovery) {
        RecoveryResponse response = new RecoveryResponse(viewNumber, recovery.nonce, opNumber,
                                                        commitNumber, replicaNumber, getLog());
        send(recovery.getConnection(), "Replica " + recovery.replicaNum, response.toString());
    }

    private long getWaitTime() {
        if (curPrimary() == replicaNumber && status == Status.NORMAL) {
            long t = timeout - (System.currentTimeMillis() - lastSentTimestamp) - 100;
            return t > 0 ? t : 0;
        } else {
            return timeout;
        }
    }

    private void timeExpired() {
        if (curPrimary() == replicaNumber) {
            Request msg = new Request(Operation.PING, new String[0], replicaNumber, commitNumber);
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

    private void startRecovery() {
        nonce = System.currentTimeMillis();
        recoveryCount = 0;
        Recovery msg = new Recovery(replicaNumber, nonce);
        broadcast(msg.toString());
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

    private void processReply(Reply reply) {
        if (reply.view > viewNumber) {
            status = Status.RECOVERING;
        }
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
                for (int i = commitNumber + 1; i <= bestCommitNum; ++i) {
                    pending.put(i, getReqFromLog(best.log.get(i - 1)));
                }
                commit(commitNumber, bestCommitNum);
            }
        }
    }

    private void rewriteLog(List<String> log) {
        Path logFile = Paths.get(logPath);
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
        if (commit.view > viewNumber) {
          status = Status.RECOVERING;
        } else {
            commit(commitNumber, commit.commitNum);
        }
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
            return Files.readAllLines(Paths.get(logPath));
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
            if (curPrimary() == replicaNumber && request.getConnection() != null) {
                send(request.getConnection(), " ", reply.toString());
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
            for (int i = commitNumber + 1; i <= startView.commitNum; ++i) {
                pending.put(i, getReqFromLog(startView.log.get(i - 1)));
            }
            commit(commitNumber, startView.commitNum);
        }
        if (opNumber > commitNumber) {
            PrepareOK msg = new PrepareOK(viewNumber, opNumber, commitNumber);
            send(connections[curPrimary() - 1], "leader", msg.toString());
        }
    }

    private Request getReqFromLog(String logEntry) {
        String[] tmp = logEntry.split(" ");
        Operation op  =  Operation.valueOf(tmp[0].toUpperCase());
        String[] args = new String[tmp.length - 1];
        System.arraycopy(tmp, 1, args, 0, tmp.length - 1);
        return new Request(op, args, -1, -1);
    }

    private void processRequest(Request request) {
        if (clientsTable.containsKey(request.clientId)) {
            ClientInfo info = clientsTable.get(request.clientId);
            if (info.lastRequestInd == request.requestNumber && info.response != null) {
                send(request.getConnection(), " " + request.clientId, info.response);
                return;
            } else if (info.lastRequestInd > request.requestNumber) {
                //drop
                return;
            }
        }

        if (request.op == Operation.GET) {
            String response = evaluate(request.args[0]);
            Reply reply = new Reply(viewNumber, request.requestNumber, response);
            send(request.getConnection(), " " + request.clientId, reply.toString());
        } else if (request.op == Operation.PING) {
            Reply reply = new Reply(viewNumber, request.requestNumber, Response.PONG.toString());
            send(request.getConnection(), ""+request.clientId, reply.toString());
        } else if (status != Status.VIEW_CHANGE){
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
        if (prepare.opNum > opNumber + 1) {
            opNumber = prepare.opNum - 1;
        }
        opNumber++;
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
        if (connection == null)
            return;
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
            try {
                ServerSocket serverSocket = new ServerSocket();
                serverSocket.setReuseAddress(true);
                serverSocket.bind(new InetSocketAddress(port));
                servSocket = serverSocket;
                while (!Thread.interrupted()) {
                    Connection conn = new Connection(serverSocket.accept(), this);
                    conn.messageQueue.offer(new Reply(viewNumber, -1, "ACCEPT").toString());
                    externalConections.add(conn);
                }
            } catch (IOException e) {
            } finally {
                try {
                    if(servSocket != null) {
                        servSocket.close();
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        acceptor.start();
        connections = new Connection[n];
        for (int i = 0; i < n; ++i) {
            if (i == replicaNumber - 1)
                continue;
            String[] address = config.getProperty("node." + (i + 1)).split(":");
            connections[i] = new Connection(address[0], Integer.valueOf(address[1]), this);
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
        logger.close();
        acceptor.interrupt();
        externalConections.forEach(Connection::close);
    }
}
