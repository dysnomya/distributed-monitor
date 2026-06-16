package chojniak.katarzyna.client;

import chojniak.katarzyna.MonitorInterface;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class Client {
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        try {
            int targetPort = 1099;
            if (args.length > 0) {
                try {
                    targetPort = Integer.parseInt(args[0]);
                } catch (NumberFormatException e) {
                    log("⚠Invalid port format provided. Falling back to default port 1099.");
                }
            }

            log("Connecting to target RMI Registry on port " + targetPort + "...");

            Registry registry = LocateRegistry.getRegistry("localhost", targetPort);
            MonitorInterface serverStub = (MonitorInterface) registry.lookup("MonitorServer");

            String serverName = serverStub.getServerName();

            System.out.println("\n=================================================");
            log("✅ Successfully established connection to the distributed monitor!");
            log("🖥️ Target Server Identity: " + serverName);
            System.out.println("=================================================\n");

            int cycle = 1;
            while (true) {
                System.out.printf("--- [Cycle #%d] ---%n", cycle);

                serverStub.acquire();

                log("🔒 [LOCK ACQUIRED] Entered Critical Section (CS) successfully!");
                log("🚀 Doing something for 5000ms...");

                Thread.sleep(5000);

                serverStub.release();

                log("🔓 [LOCK RELEASED] Exited Critical Section. Node is idle.");
                System.out.println("-------------------\n");

                cycle++;
                Thread.sleep(2000);
            }

        } catch (Exception e) {
            System.err.println("\n[FATAL] Client runtime encountered a connection drop or failure:");
            e.printStackTrace();
        }
    }

    private static void log(String message) {
        String timestamp = LocalTime.now().format(TIME_FORMATTER);
        System.out.printf("[%s][Client] %s%n", timestamp, message);
    }
}