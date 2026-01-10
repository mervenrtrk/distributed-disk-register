package com.example.family;

import family.*;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.*;
import java.util.Map;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.io.IOException;

public class FamilyServiceImpl extends FamilyServiceGrpc.FamilyServiceImplBase {

    private final NodeRegistry registry;
    private final NodeInfo self;
    private final boolean isLeader;

    // RAM storage
    private final Map<Long, String> localStore = new ConcurrentHashMap<>();

    // Disk storage
    private final Path storageDir;

    public FamilyServiceImpl(NodeRegistry registry,
                             NodeInfo self,
                             boolean isLeader) {

        this.registry = registry;
        this.self = self;
        this.isLeader = isLeader;

        registry.add(self);

        this.storageDir = Paths.get(
                "data",
                self.getHost() + "_" + self.getPort()
        );

        if (!isLeader) {
            try {
                Files.createDirectories(storageDir);
                loadFromDisk();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        startLocalStatsPrinter();
    }

    // JOIN

    @Override
    public void join(NodeInfo request,
                     StreamObserver<FamilyView> responseObserver) {

        registry.add(request);

        responseObserver.onNext(
                FamilyView.newBuilder()
                        .addAllMembers(registry.snapshot())
                        .build()
        );
        responseObserver.onCompleted();
    }

   
    // SET (FOLLOWER ONLY)
    @Override
    public void receiveChat(ChatMessage request,
                            StreamObserver<Empty> responseObserver) {

        if (isLeader) {
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
            return;
        }

        String[] parts = request.getText().split(" ", 3);
        if (parts.length == 3 && parts[0].equals("SET")) {

            long id = Long.parseLong(parts[1]);
            String value = parts[2];

            // RAM
            localStore.put(id, value);

            // DISK
            try {
                Files.writeString(
                        storageDir.resolve(id + ".txt"),
                        value,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING
                );
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

    // GET
    @Override
    public void getValue(GetRequest request,
                         StreamObserver<GetResponse> responseObserver) {

        String val = localStore.get(request.getId());

        responseObserver.onNext(
                val != null
                        ? GetResponse.newBuilder()
                            .setFound(true)
                            .setValue(val)
                            .build()
                        : GetResponse.newBuilder()
                            .setFound(false)
                            .build()
        );
        responseObserver.onCompleted();
    }

    // DISK LOAD

    private void loadFromDisk() {
        try {
            Files.list(storageDir).forEach(p -> {
                try {
                    long id = Long.parseLong(
                            p.getFileName().toString().replace(".txt", "")
                    );
                    localStore.put(
                            id,
                            Files.readString(p, StandardCharsets.UTF_8)
                    );
                } catch (Exception ignored) {}
            });
            System.out.println("📂 Diskten veri yüklendi");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // STATS
    private void startLocalStatsPrinter() {
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(() -> {
                    System.out.println("---- NODE ----");
                    System.out.println("Self: " + self.getHost() + ":" + self.getPort());
                    System.out.println("Leader: " + isLeader);
                    System.out.println("Local keys: " + localStore.size());
                }, 5, 10, TimeUnit.SECONDS);
    }
}
