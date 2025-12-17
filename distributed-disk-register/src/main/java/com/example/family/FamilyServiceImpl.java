package com.example.family;

import family.Empty;
import family.FamilyServiceGrpc;
import family.FamilyView;
import family.NodeInfo;
import family.ChatMessage;
import io.grpc.stub.StreamObserver;

public class FamilyServiceImpl extends FamilyServiceGrpc.FamilyServiceImplBase {

    private final NodeRegistry registry;
    private final NodeInfo self;

    public FamilyServiceImpl(NodeRegistry registry, NodeInfo self) {
        this.registry = registry;
        this.self = self;
        this.registry.add(self);
    }

    @Override
    public void join(NodeInfo request, StreamObserver<FamilyView> responseObserver) {
        registry.add(request);

        FamilyView view = FamilyView.newBuilder()
                .addAllMembers(registry.snapshot())
                .build();

        responseObserver.onNext(view);
        responseObserver.onCompleted();
    }

    @Override
    public void getFamily(Empty request, StreamObserver<FamilyView> responseObserver) {
        FamilyView view = FamilyView.newBuilder()
                .addAllMembers(registry.snapshot())
                .build();

        responseObserver.onNext(view);
        responseObserver.onCompleted();
    }

    // Diğer düğümlerden broadcast mesajı geldiğinde
@Override
public void receiveChat(ChatMessage request, StreamObserver<Empty> responseObserver) {

    String text = request.getText();
    System.out.println("💬 Incoming replication: " + text);

    String[] parts = text.split(" ", 3);

    if (parts.length == 3 && parts[0].equalsIgnoreCase("SET")) {
        try {
            long id = Long.parseLong(parts[1]);
            String value = parts[2];

            // ✅ FOLLOWER MEMORY WRITE
            NodeMain.putToMemory(id, value);

            System.out.printf("✅ Replica stored: %d = %s%n", id, value);

        } catch (NumberFormatException e) {
            System.err.println("❌ Invalid SET replication message");
        }
    }

    responseObserver.onNext(Empty.newBuilder().build());
    responseObserver.onCompleted();
}


}
