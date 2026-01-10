package com.example.family;

import java.io.*;
import java.net.Socket;

public class SimpleClient {

    public static void main(String[] args) throws Exception {

        try (Socket socket = new Socket("127.0.0.1", 6666);
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(
                     socket.getOutputStream(), true);
             BufferedReader stdin = new BufferedReader(
                     new InputStreamReader(System.in))
        ) {
            System.out.println("Connected to leader. Type commands:");

            String line;
            while ((line = stdin.readLine()) != null) {

                out.println(line);              //  Server’a gönder
                String response = in.readLine(); //  Server’dan cevap

                if (response == null) break;
                System.out.println("Server: " + response);
            }
        }
    }
}
