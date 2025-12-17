package com.example.family;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ToleranceConfig {

    private static final String CONFIG_FILE = "tolerance.conf";

    public static int load() {
        int tolerance = 1; // default (failsafe)

        try (BufferedReader br = new BufferedReader(
                new FileReader(CONFIG_FILE))) {

            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();

                if (line.startsWith("tolerance=")) {
                    String value = line.split("=", 2)[1];
                    tolerance = Integer.parseInt(value);
                }
            }

        } catch (IOException e) {
            System.err.println(
                "WARNING: tolerance.conf not found, using default tolerance=1");
        }

        return tolerance;
    }
}
