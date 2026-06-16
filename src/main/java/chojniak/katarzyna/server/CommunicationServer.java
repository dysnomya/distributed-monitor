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
    private final ReplicatedBufferMonitor<T> buffer;

    protected CommunicationServer(String name, int bufferCapacity) throws RemoteException {

        super();
        this.name = name;
        this.lock = new DistributedLock(name);
        this.buffer = new ReplicatedBufferMonitor<>(bufferCapacity);
    }

    @Override
    public String getServerName() throws RemoteException {
        return name;
    }

    @Override
    public void put(T value) throws RemoteException {
        try {
            lock.acquire();
            while (buffer.isFull()) {
                lock.release();
                buffer.waitNotFull();
                lock.acquire();
            }
            buffer.put(value);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.release();
        }
    }

    @Override
    public T get() throws RemoteException {
        try {
            lock.acquire();
            while (buffer.isEmpty()) {
                lock.release();
                buffer.waitNotEmpty();
                lock.acquire();
            }
            return buffer.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } finally {
            lock.release();
        }
    }

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

    @Override
    public synchronized void registerPeer(String name, CommunicationInterface<T> peer) throws RemoteException {
        lock.addPeer(name, peer);
        buffer.addPeer(peer);
        System.out.println("[PEER] registered " + name);
    }

    @Override
    public void handleNewPeer(String peerId, String peerIp, int peerPort) throws RemoteException {
        try {

            Registry registry = LocateRegistry.getRegistry(peerIp, peerPort);
            CommunicationInterface<T> peer = (CommunicationInterface) registry.lookup("MonitorServer");
            registerPeer(peerId, peer);

            peer.registerPeer(name, this);
        } catch (Exception e) {
            System.err.println("Peer registration failed: " + peerId);
        }
    }
}