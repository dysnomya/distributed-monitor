package chojniak.katarzyna.server;

import java.net.*;

public class NetworkDiscovery {
    private static final String MULTICAST_GROUP = "230.0.0.1";
    private static final int PORT = 1900;

    public static void notifyArrival(String serverId, String myIp, int rmiPort) {
        try (DatagramSocket socket = new DatagramSocket()) {
            InetAddress group = InetAddress.getByName(MULTICAST_GROUP);
            String payload = serverId + ":" + myIp + ":" + rmiPort;
            byte[] buffer = payload.getBytes();

            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group, PORT);
            socket.send(packet);
            System.out.println("[Discovery] Sent single arrival notification to the network.");
        } catch (Exception e) {
            System.err.println("[Discovery Error] Failed to broadcast arrival: " + e.getMessage());
        }
    }

    public static void startListener(String serverId, DistributedServer server) {
        new Thread(() -> {
            try (MulticastSocket socket = new MulticastSocket(null)) {
                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress(PORT));

                InetAddress group = InetAddress.getByName(MULTICAST_GROUP);
                socket.joinGroup(new InetSocketAddress(group, PORT), findMulticastInterface());

                byte[] buffer = new byte[1024];
                System.out.println("[Discovery] Listener active. Waiting for new servers to join...");

                while (true) {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);

                    String message = new String(packet.getData(), 0, packet.getLength()).trim();
                    String[] parts = message.split(":");
                    if (parts.length != 3) {
                        continue;
                    }

                    String peerId = parts[0];
                    String peerIp = parts[1];
                    int peerPort = Integer.parseInt(parts[2]);

                    if (!peerId.equals(serverId)) {
                        server.handleNewPeer(peerId, peerIp, peerPort);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static NetworkInterface findMulticastInterface() throws Exception {
        return NetworkInterface.getByInetAddress(InetAddress.getLocalHost());
    }
}


