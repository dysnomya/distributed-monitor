package chojniak.katarzyna.client;

import chojniak.katarzyna.MonitorInterface;

import java.io.Serializable;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class MonitorClient<T extends Serializable> {
    private final MonitorInterface<T> server;

    private MonitorClient(MonitorInterface<T> server) {
        this.server = server;
    }

    public static <T extends Serializable> MonitorClient<T> connect(String host, int port) throws Exception {
        Registry registry = LocateRegistry.getRegistry(host, port);
        MonitorInterface<T> stub = (MonitorInterface<T>) registry.lookup("MonitorServer");

        return new MonitorClient<>(stub);
    }
}
