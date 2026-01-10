package com.example.family;

import family.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class NodeMain {

    private static final int START_PORT = 5555;
    private static final int CLIENT_PORT = 6666;

    // message_id -> hangi node’larda tutuluyor
    private static final Map<Long, List<NodeInfo>> messageLocations =
            new ConcurrentHashMap<>();

    private static final AtomicInteger rrIndex = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {

        String host = "127.0.0.1";
        int port = findFreePort(START_PORT);

        NodeInfo self = NodeInfo.newBuilder()
                .setHost(host)
                .setPort(port)
                .build();

        boolean isLeader = (port == START_PORT);
        int tolerance = 1;

        if (isLeader) {
            tolerance = ToleranceConfig.load();
            System.out.println("LEADER tolerance = " + tolerance);
        }

        NodeRegistry registry = new NodeRegistry();

        FamilyServiceImpl service = new FamilyServiceImpl(registry, self, isLeader);

        Server server = ServerBuilder
                .forPort(port)
                .addService(service)
                .build()
                .start();

        System.out.printf("Node started at %s:%d%n", host, port);

        discoverExistingNodes(host, port, registry, self);
        startFamilyPrinter(registry);

        if (isLeader) {
            startLeaderTextListener(registry, self, tolerance);
            startLeaderStatsPrinter();
        }

        server.awaitTermination();
    }

    // ======================================================
    // LEADER TCP HANDLER
    // ======================================================

    private static void startLeaderTextListener(NodeRegistry registry,
                                                NodeInfo self,
                                                int tolerance) {

        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(CLIENT_PORT)) {
                System.out.println("Leader listening on TCP " + CLIENT_PORT);

                while (true) {
                    Socket client = serverSocket.accept();
                    new Thread(() ->
                            handleClient(client, registry, self, tolerance)
                    ).start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void handleClient(Socket client,
                                     NodeRegistry registry,
                                     NodeInfo self,
                                     int tolerance) {

        try (BufferedReader in = new BufferedReader(
                new InputStreamReader(client.getInputStream()));
             PrintWriter out = new PrintWriter(
                     client.getOutputStream(), true)) {

            String line;
            while ((line = in.readLine()) != null) {

                String[] parts = line.split(" ", 3);
                String cmd = parts[0].toUpperCase();

                if (cmd.equals("SET")) {
                    long id = Long.parseLong(parts[1]);
                    String value = parts[2];

                    boolean ok = replicateSet(registry, self, id, value, tolerance);
                    out.println(ok ? "OK" : "ERROR");

                } else if (cmd.equals("GET")) {
                    long id = Long.parseLong(parts[1]);

                    String val = fetchValue(registry, id);
                    out.println(val == null
                            ? "NOT_FOUND"
                            : "VALUE " + id + " " + val);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // SET REPLICATION (LEADER)

private static boolean replicateSet(NodeRegistry registry,
                                    NodeInfo self,
                                    long id,
                                    String value,
                                    int tolerance) {

    List<NodeInfo> candidates = registry.snapshot()
            .stream()
            .filter(n -> !(n.getHost().equals(self.getHost())
                        && n.getPort() == self.getPort()))
            .toList();

    if (candidates.size() < tolerance) return false;

    List<NodeInfo> selected = selectRoundRobin(candidates, tolerance);
    List<NodeInfo> stored = new ArrayList<>();

    for (NodeInfo n : selected) {
        try {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(n.getHost(), n.getPort())
                    .usePlaintext()
                    .build();

            FamilyServiceGrpc.FamilyServiceBlockingStub stub =
                    FamilyServiceGrpc.newBlockingStub(channel);

            stub.receiveChat(
                    ChatMessage.newBuilder()
                            .setText("SET " + id + " " + value)
                            .build()
            );

            channel.shutdownNow();
            stored.add(n);

        } catch (Exception ignored) {}
    }

    if (stored.size() >= tolerance) {
        messageLocations.put(id, stored);
        return true;
    }
    return false;
}

    // GET

    private static String fetchValue(NodeRegistry registry, long id) {

        List<NodeInfo> holders = messageLocations.get(id);
        if (holders == null) return null;

        for (NodeInfo n : holders) {
            if (!registry.snapshot().contains(n)) continue;

            try {
                ManagedChannel channel = ManagedChannelBuilder
                        .forAddress(n.getHost(), n.getPort())
                        .usePlaintext()
                        .build();

                FamilyServiceGrpc.FamilyServiceBlockingStub stub =
                        FamilyServiceGrpc.newBlockingStub(channel);

                GetResponse resp = stub.getValue(
                        GetRequest.newBuilder().setId(id).build());

                channel.shutdownNow();

                if (resp.getFound()) return resp.getValue();

            } catch (Exception ignored) {}
        }
        return null;
    }

    // UTIL
    
    private static List<NodeInfo> selectRoundRobin(List<NodeInfo> list, int k) {
        List<NodeInfo> res = new ArrayList<>();
        int start = rrIndex.getAndAdd(k);

        for (int i = 0; i < k; i++) {
            res.add(list.get((start + i) % list.size()));
        }
        return res;
    }

    private static void discoverExistingNodes(String host,
                                              int selfPort,
                                              NodeRegistry registry,
                                              NodeInfo self) {

        for (int port = START_PORT; port < selfPort; port++) {
            try {
                ManagedChannel channel = ManagedChannelBuilder
                        .forAddress(host, port)
                        .usePlaintext()
                        .build();

                FamilyServiceGrpc.FamilyServiceBlockingStub stub =
                        FamilyServiceGrpc.newBlockingStub(channel);

                FamilyView view = stub.join(self);
                registry.addAll(view.getMembersList());

                channel.shutdownNow();
            } catch (Exception ignored) {}
        }
    }

    private static int findFreePort(int start) {
        int p = start;
        while (true) {
            try (ServerSocket s = new ServerSocket(p)) {
                return p;
            } catch (IOException e) {
                p++;
            }
        }
    }

    private static void startFamilyPrinter(NodeRegistry registry) {
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(() -> {
                    System.out.println("---- FAMILY ----");
                    registry.snapshot().forEach(n ->
                            System.out.println(n.getHost() + ":" + n.getPort()));
                }, 5, 10, TimeUnit.SECONDS);
    }

    private static void startLeaderStatsPrinter() {
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(() -> {
                    System.out.println("---- LEADER STATS ----");
                    System.out.println("Messages stored: " + messageLocations.size());
                }, 5, 10, TimeUnit.SECONDS);
    }
}
