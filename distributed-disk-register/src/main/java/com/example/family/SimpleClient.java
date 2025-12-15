package com.example.family;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class SimpleClient {

    public static void main(String[] args) {
        try {
            String host = "127.0.0.1";
            int port = 6666;

            Socket socket = new Socket(host, port);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            Scanner scanner = new Scanner(System.in);

            System.out.println("Connected to HaToKuSe Leader");
            System.out.println("Commands:");
            System.out.println("  SET <id> <message>");
            System.out.println("  GET <id>");
            System.out.println("  exit");

            while (true) {
                System.out.print("> ");
                String line = scanner.nextLine();

                if (line.equalsIgnoreCase("exit")) {
                    break;
                }

                out.println(line);
            }

            socket.close();
            System.out.println("Disconnected.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}