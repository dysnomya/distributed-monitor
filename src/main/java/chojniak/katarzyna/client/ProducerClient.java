package chojniak.katarzyna.client;

import chojniak.katarzyna.MonitorInterface;
import org.kohsuke.randname.RandomNameGenerator;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class ProducerClient {
    private static volatile boolean running = true;

    public static void main(String[] args) {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Client stopping...");
                running = false;
            }));

            int targetPort = 1099;
            if (args.length > 0) {
                try {
                    targetPort = Integer.parseInt(args[0]);
                } catch (NumberFormatException e) {
                    System.out.println("Invalid port format provided. Falling back to default port 1099.");
                }
            }

            System.out.println("Connecting to target RMI Registry on port " + targetPort);
            Registry registry = LocateRegistry.getRegistry("localhost", targetPort);
            MonitorInterface monitor = (MonitorInterface) registry.lookup("MonitorServer");

            System.out.println("\n=================================================");
            System.out.println("Successfully established connection to the distributed monitor!");
            System.out.println("Target Server: " + monitor.getServerName());
            System.out.println("=================================================\n");

            int cycle = 1;
            RandomNameGenerator generator = new RandomNameGenerator();
            while (running) {
                System.out.printf("--- [Cycle #%d] ---%n", cycle);
                String putString = "hello " + generator.next();
                monitor.put(putString);
                System.out.println("put: " + putString);
                cycle++;
                Thread.sleep(5000);
            }

        } catch (Exception e) {
            System.err.println("\n[FATAL] Client runtime encountered a connection drop or failure:");
            e.printStackTrace();
        }
    }
}
