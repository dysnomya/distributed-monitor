package chojniak.katarzyna.server;

import chojniak.katarzyna.CommunicationInterface;
import chojniak.katarzyna.MonitorInterface;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class CommunicationServer<T extends Serializable> extends UnicastRemoteObject implements MonitorInterface<T>, CommunicationInterface<T> {

    private final String name;
    private final DistributedLock lock;
    private final ReplicatedBuffer<T> buffer;
    private final DistributedMonitor<T> monitor;

    protected CommunicationServer(String name, int bufferCapacity) throws RemoteException {
        super();
        this.name = name;
        this.lock = new DistributedLock(name);
        this.buffer = new ReplicatedBuffer<>(bufferCapacity);
        this.monitor = new DistributedMonitor<>(this.lock, this.buffer);
    }

    // =========================================================================
    // CLIENT INTERFACE IMPLEMENTATION (MonitorInterface)
    // =========================================================================

    @Override
    public String getServerName() throws RemoteException {
        return name;
    }

    @Override
    public void put(T value) throws RemoteException {
        try {
            monitor.put(value);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteException("Put operation interrupted", e);
        }
    }

    @Override
    public T get() throws RemoteException {
        try {
            return monitor.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteException("Get operation interrupted", e);
        }
    }

    // =========================================================================
    // PEER INTERFACE IMPLEMENTATION (CommunicationInterface)
    // =========================================================================

    @Override
    public void replicatePut(T value) throws RemoteException {
        buffer.replicatePut(value);
    }

    @Override
    public void replicateGet() throws RemoteException {
        buffer.replicateGet();
    }

    @Override
    public void requestLock(String sender, int timestamp) throws RemoteException {
        lock.request(sender, timestamp);
    }

    @Override
    public void agreeLock(String sender, int timestamp) throws RemoteException {
        lock.agree(sender, timestamp);
    }

    // =========================================================================
    // CLUSTER DISCOVERY
    // =========================================================================

    @Override
    public void registerPeer(String peerName, CommunicationInterface<T> peerStub) throws RemoteException {
        lock.addPeer(peerName, peerStub);
        buffer.addPeer(peerStub);
        System.out.println("[CLUSTER] Peer successfully linked: " + peerName);
    }

    @Override
    public void handleNewPeer(String peerId, String peerIp, int peerPort) throws RemoteException {
        try {
            Registry registry = LocateRegistry.getRegistry(peerIp, peerPort);
            CommunicationInterface<T> peerStub = (CommunicationInterface<T>) registry.lookup("MonitorServer");

            registerPeer(peerId, peerStub);
            peerStub.registerPeer(this.name, this);

        } catch (Exception e) {
            System.err.println("[CLUSTER] Failed to connect to bootstrapper peer: " + e);
        }
    }
}