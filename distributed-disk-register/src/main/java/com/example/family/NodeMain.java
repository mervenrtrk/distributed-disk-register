package com.example.family;

import family.Empty;
import family.FamilyServiceGrpc;
import family.FamilyView;
import family.NodeInfo;
import family.ChatMessage;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;


import java.io.IOException;
import java.net.ServerSocket;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.*;


public class NodeMain {

    private static final int START_PORT = 5555;
    private static final int PRINT_INTERVAL_SECONDS = 10;
    private static final ConcurrentMap<Long, String> memoryRegister = new ConcurrentHashMap<>();


    public static void putToMemory(long id, String value) {
    memoryRegister.put(id, value);
}

    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = findFreePort(START_PORT);

        NodeInfo self = NodeInfo.newBuilder()
                .setHost(host)
                .setPort(port)
                .build();
    int tolerance = 1; // default
        if (port == START_PORT) {
            tolerance = ToleranceConfig.load();
            System.out.println("Leader tolerance level = " + tolerance);
    }


        NodeRegistry registry = new NodeRegistry();
        FamilyServiceImpl service = new FamilyServiceImpl(registry, self);

        Server server = ServerBuilder
                .forPort(port)
                .addService(service)
                .build()
                .start();

                System.out.printf("Node started on %s:%d%n", host, port);

                // Eğer bu ilk node ise (port 5555), TCP 6666'da text dinlesin
                if (port == START_PORT) {
                    startLeaderTextListener(registry, self, tolerance);
                }

                discoverExistingNodes(host, port, registry, self);
                startFamilyPrinter(registry, self);
                startHealthChecker(registry, self);

                server.awaitTermination();




    }

private static void startLeaderTextListener(NodeRegistry registry,
                                            NodeInfo self,
                                            int tolerance) {

    new Thread(() -> {
        try (ServerSocket serverSocket = new ServerSocket(6666)) {
            System.out.printf("Leader listening for text on TCP %s:%d%n",
                    self.getHost(), 6666);

            while (true) {
                Socket client = serverSocket.accept();
                new Thread(() ->
                    handleClientTextConnection(client, registry, self, tolerance)
                ).start();
            }

        } catch (IOException e) {
            System.err.println("Error in leader text listener: " + e.getMessage());
        }
    }, "LeaderTextListener").start();
}

 private static boolean replicateSetToFamily(NodeRegistry registry,
                                             NodeInfo self,
                                             long id,
                                             String value,
                                             int tolerance) {

    int success = 1; // leader itself

    List<NodeInfo> members = registry.snapshot();

    for (NodeInfo n : members) {

        if (n.getHost().equals(self.getHost()) &&
            n.getPort() == self.getPort()) {
            continue;
        }

        try {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(n.getHost(), n.getPort())
                    .usePlaintext()
                    .build();

            FamilyServiceGrpc.FamilyServiceBlockingStub stub =
                    FamilyServiceGrpc.newBlockingStub(channel);

            ChatMessage msg = ChatMessage.newBuilder()
                    .setText("SET " + id + " " + value)
                    .setFromHost(self.getHost())
                    .setFromPort(self.getPort())
                    .setTimestamp(System.currentTimeMillis())
                    .build();

            stub.receiveChat(msg);
            success++;

        } catch (Exception e) {
            // follower unreachable → ignore
        }
    }

    return success >= (tolerance + 1);
}
 




private static void handleClientTextConnection(Socket client,
                                               NodeRegistry registry,
                                               NodeInfo self, int tolerance) {
    System.out.println("New TCP client connected: " + client.getRemoteSocketAddress());
    try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(client.getInputStream()))) {

        String line;
        while ((line = reader.readLine()) != null) {
            String text = line.trim();
            if (text.isEmpty()) continue;

            long ts = System.currentTimeMillis();

            
            System.out.println("Received from TCP: " + text);
            String[] parts = text.split(" ", 3);
            String command = parts[0].toUpperCase();
if (command.equals("SET")) {

    if (parts.length < 3) {
        client.getOutputStream().write(
            "ERROR Invalid SET format\n".getBytes());
        continue;
    }

    long messageId;
    try {
        messageId = Long.parseLong(parts[1]);
    } catch (NumberFormatException e) {
        client.getOutputStream().write(
            "ERROR Invalid ID\n".getBytes());
        continue;
    }

    String message = parts[2];
  boolean committed = replicateSetToFamily(
            registry,
            self,
            messageId,
            message,
            tolerance
    );

    if (committed) {
        // ✅ SADECE BURADA MEMORY'YE YAZ
        memoryRegister.put(messageId, message);

        client.getOutputStream().write(
            ("OK SET " + messageId + "\n").getBytes());
    } else {
        client.getOutputStream().write(
            "ERROR Quorum not reached\n".getBytes());
    }
}


else if (command.equals("GET")) {

    if (parts.length < 2) {
        client.getOutputStream().write(
            "ERROR Invalid GET format\n".getBytes());
        continue;
    }

    long messageId;
    try {
        messageId = Long.parseLong(parts[1]);
    } catch (NumberFormatException e) {
        client.getOutputStream().write(
            "ERROR Invalid ID\n".getBytes());
        continue;
    }

    String value = memoryRegister.get(messageId);

    if (value == null) {
        client.getOutputStream().write(
            ("NOT_FOUND " + messageId + "\n").getBytes());
    } else {
        client.getOutputStream().write(
            ("VALUE " + messageId + " " + value + "\n").getBytes());
    }
}
else if (command.equals("EXIT")) {

    System.out.println("Client requested exit.");
    break;
}


else {
    client.getOutputStream().write(
        ("ERROR Unknown command\n").getBytes());
}


          /*   ChatMessage msg = ChatMessage.newBuilder()
                    .setText(text)
                    .setFromHost(self.getHost())
                    .setFromPort(self.getPort())
                    .setTimestamp(ts)
                    .build();

            // Tüm family üyelerine broadcast et
            broadcastToFamily(registry, self, msg);*/
        }

    } catch (IOException e) {
        System.err.println("TCP client handler error: " + e.getMessage());
    } finally {
        try { client.close(); } catch (IOException ignored) {}
    }
}






private static void broadcastToFamily(NodeRegistry registry,
                                      NodeInfo self,
                                      ChatMessage msg) {

    List<NodeInfo> members = registry.snapshot();

    for (NodeInfo n : members) {
        // Kendimize tekrar gönderme
        if (n.getHost().equals(self.getHost()) && n.getPort() == self.getPort()) {
            continue;
        }

        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder
                    .forAddress(n.getHost(), n.getPort())
                    .usePlaintext()
                    .build();

            FamilyServiceGrpc.FamilyServiceBlockingStub stub =
                    FamilyServiceGrpc.newBlockingStub(channel);

            stub.receiveChat(msg);

            System.out.printf("Broadcasted message to %s:%d%n", n.getHost(), n.getPort());

        } catch (Exception e) {
            System.err.printf("Failed to send to %s:%d (%s)%n",
                    n.getHost(), n.getPort(), e.getMessage());
        } finally {
            if (channel != null) channel.shutdownNow();
        }
    }
}


    private static int findFreePort(int startPort) {
        int port = startPort;
        while (true) {
            try (ServerSocket ignored = new ServerSocket(port)) {
                return port;
            } catch (IOException e) {
                port++;
            }
        }
    }

    private static void discoverExistingNodes(String host,
                                              int selfPort,
                                              NodeRegistry registry,
                                              NodeInfo self) {

        for (int port = START_PORT; port < selfPort; port++) {
            ManagedChannel channel = null;
            try {
                channel = ManagedChannelBuilder
                        .forAddress(host, port)
                        .usePlaintext()
                        .build();

                FamilyServiceGrpc.FamilyServiceBlockingStub stub =
                        FamilyServiceGrpc.newBlockingStub(channel);

                FamilyView view = stub.join(self);
                registry.addAll(view.getMembersList());

                System.out.printf("Joined through %s:%d, family size now: %d%n",
                        host, port, registry.snapshot().size());

            } catch (Exception ignored) {
            } finally {
                if (channel != null) channel.shutdownNow();
            }
        }
    }

    private static void startFamilyPrinter(NodeRegistry registry, NodeInfo self) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {
            List<NodeInfo> members = registry.snapshot();
            System.out.println("======================================");
            System.out.printf("Family at %s:%d (me)%n", self.getHost(), self.getPort());
            System.out.println("Time: " + LocalDateTime.now());
            System.out.println("Members:");

            for (NodeInfo n : members) {
                boolean isMe = n.getHost().equals(self.getHost()) && n.getPort() == self.getPort();
                System.out.printf(" - %s:%d%s%n",
                        n.getHost(),
                        n.getPort(),
                        isMe ? " (me)" : "");
            }
            System.out.println("======================================");
        }, 3, PRINT_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private static void startHealthChecker(NodeRegistry registry, NodeInfo self) {
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    scheduler.scheduleAtFixedRate(() -> {
        List<NodeInfo> members = registry.snapshot();

        for (NodeInfo n : members) {
            // Kendimizi kontrol etmeyelim
            if (n.getHost().equals(self.getHost()) && n.getPort() == self.getPort()) {
                continue;
            }

            ManagedChannel channel = null;
            try {
                channel = ManagedChannelBuilder
                        .forAddress(n.getHost(), n.getPort())
                        .usePlaintext()
                        .build();

                FamilyServiceGrpc.FamilyServiceBlockingStub stub =
                        FamilyServiceGrpc.newBlockingStub(channel);

                // Ping gibi kullanıyoruz: cevap bizi ilgilendirmiyor,
                // sadece RPC'nin hata fırlatmaması önemli.
                stub.getFamily(Empty.newBuilder().build());

            } catch (Exception e) {
                // Bağlantı yok / node ölmüş → listeden çıkar
                System.out.printf("Node %s:%d unreachable, removing from family%n",
                        n.getHost(), n.getPort());
                registry.remove(n);
            } finally {
                if (channel != null) {
                    channel.shutdownNow();
                }
            }
        }

    }, 5, 10, TimeUnit.SECONDS); // 5 sn sonra başla, 10 sn'de bir kontrol et
}

}
