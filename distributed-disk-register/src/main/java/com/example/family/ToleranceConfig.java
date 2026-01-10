package com.example.family;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ToleranceConfig {

    private static final String CONFIG_FILE = "tolerance.conf";

    public static int load() {
        int tolerance = 1; // default

        try {
            InputStream is = ToleranceConfig.class
                    .getClassLoader()
                    .getResourceAsStream(CONFIG_FILE);

            if (is == null) {
                System.err.println(
                        "WARNING: tolerance.conf not found in resources, using default=1");
                return tolerance;
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line;

            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("tolerance=")) {
                    tolerance = Integer.parseInt(
                            line.split("=", 2)[1]
                    );
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return tolerance;
    }
}
