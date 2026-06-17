package chojniak.katarzyna.client;

import chojniak.katarzyna.server.CommunicationInterface;
import chojniak.katarzyna.server.DistributedMonitorImpl;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class ConsumerClient {
    private static volatile boolean running = true;
    private static boolean bufferHasData = false;

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
            DistributedMonitor monitor = new DistributedMonitorImpl((CommunicationInterface) registry.lookup("MonitorServer"));

            System.out.println("\n=================================================");
            System.out.println("Successfully established connection to the distributed monitor!");
//            System.out.println("Target Server: " + monitor.getServerName());
            System.out.println("=================================================\n");

            DistributedMonitor.DistributedCondition notEmpty = monitor.newCondition("NotEmpty");

            int cycle = 1;
            while (running) {
                monitor.lock();

                try {
                    System.out.println("[LOCK]");
                    while (!bufferHasData) {
                        System.out.println("[AWAIT] " + "NotEmpty");
                        notEmpty.await();
                        System.out.println("[WOKE UP]");
                        bufferHasData = true;
                    }

                    bufferHasData = false;

                } finally {
                    monitor.unlock();
                    System.out.println("[UNLOCK]");
                }
                System.out.println("-----------------------------------------\n");

                cycle++;
                Thread.sleep(5000); // Wait 5 seconds before the next loop iteration
//                System.out.printf("--- [Cycle #%d] ---%n", cycle);
//
//                System.out.println("received: " + monitor.get());
//                System.out.println("-------------------\n");
//
//                cycle++;
//                Thread.sleep(5000);
            }

        } catch (Exception e) {
            System.err.println("\n[FATAL] Client runtime encountered a connection drop or failure:");
            e.printStackTrace();
        }
    }
}