package chojniak.katarzyna.server;

import org.kohsuke.randname.RandomNameGenerator;

import java.net.InetAddress;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Server {
    static void main(String[] args) {
        try {
            int rmiPort = 1099;
            if (args.length > 0) {
                try {
                    rmiPort = Integer.parseInt(args[0]);
                } catch (NumberFormatException e) {
                    System.out.println("[System] Invalid port format provided. Falling back to 1099.");
                }
            }

            String myIp = InetAddress.getLocalHost().getHostAddress();
            System.setProperty("java.rmi.server.hostname", myIp);
            CommunicationServer server = new CommunicationServer(new RandomNameGenerator().next());

            Registry registry = LocateRegistry.createRegistry(rmiPort);
            registry.rebind("MonitorServer", server);

            System.out.println("=================================================");
            System.out.println("[READY] Communication Server is running!");
            System.out.println("[INFO] IP Address: " + myIp);
            System.out.println("[INFO] RMI Port:   " + rmiPort);
            System.out.println("[INFO] Server ID:  " + server.getServerName());
            System.out.println("=================================================");

            NetworkDiscovery.startListener(server);
            Thread.sleep(500);

            NetworkDiscovery.notifyArrival(server.getServerName(), myIp, rmiPort);

        } catch (Exception e) {
            System.err.println("[FATAL] Server failed to launch:");
            e.printStackTrace();
        }
    }}
