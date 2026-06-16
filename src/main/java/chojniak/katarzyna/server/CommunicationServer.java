package chojniak.katarzyna.server;

import chojniak.katarzyna.MonitorInterface;
import chojniak.katarzyna.State;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

public class CommunicationServer extends UnicastRemoteObject implements MonitorInterface {
    private final String name;
    private final LogicalClock clock = new LogicalClock();

    private final ReentrantLock localQueueLock = new ReentrantLock();
    private State state = State.RELEASED;
    private int myRequestTimestamp = 0;
    private CountDownLatch approvalLatch;

    private final ConcurrentMap<String, MonitorInterface> peers = new ConcurrentHashMap<>();
    private final Set<String> deferredRequests = ConcurrentHashMap.newKeySet();

    protected CommunicationServer(String name) throws RemoteException {
        super();
        this.name = name;
    }

    @Override
    public void acquire() throws RemoteException {
        localQueueLock.lock();

        int peersNeeded;
        int myTimestamp;

        synchronized (this) {
            this.state = State.WANTED;
            this.myRequestTimestamp = clock.increment();
            myTimestamp = this.myRequestTimestamp;
            peersNeeded = peers.size();

            this.approvalLatch = new CountDownLatch(peersNeeded);
        }

        if (peersNeeded == 0) {
            synchronized (this) {
                this.state = State.HELD;
            }
            return;
        }

        for (String peer : peers.keySet()) {
            try {
                peers.get(peer).requestLock(name, myTimestamp);
            } catch (Exception e) {
                System.err.println(String.format("[%s][ERROR] Could not reach peer '%s'. Dropping latch count fallback.", name, peer));
                approvalLatch.countDown();
            }
        }

        try {
            approvalLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log("[ERROR] Acquire thread interrupted while sleeping.");
        }
        synchronized (this) {
            this.state = State.HELD;
        }
    }

    @Override
    public synchronized void release() throws RemoteException {
        Set<String> targetsToNotify;

        synchronized (this) {
            this.state = State.RELEASED;
            targetsToNotify = ConcurrentHashMap.newKeySet();
            targetsToNotify.addAll(deferredRequests);
            deferredRequests.clear();
        }

        for (String peer : targetsToNotify) {
            try {
                MonitorInterface peerStub = peers.get(peer);
                if (peerStub != null) {
                    int responseTicket = clock.increment();
                    peerStub.agreeLock(this.name, responseTicket);
                }
            } catch (RemoteException e) {
                System.err.printf("[%s][ERROR] Failed to forward release response to peer '%s'%n", name, peer);
            }
        }

        localQueueLock.unlock();
    }

    @Override
    public void requestLock(String sender, int timestamp) throws RemoteException {
        clock.update(timestamp);

        boolean shouldDefer;
        synchronized (this) {
            shouldDefer = switch (this.state) {
                case HELD -> true;
                case WANTED -> timestampIsOlder(timestamp, sender);
                case RELEASED -> false;
            };
        }

        if (shouldDefer) {
            deferredRequests.add(sender);
        } else {
            MonitorInterface peerStub = peers.get(sender);
            if (peerStub != null) {
                peerStub.agreeLock(this.name, clock.getValue());
            }
        }
    }

    @Override
    public void agreeLock(String server, int timestamp) throws RemoteException {
        clock.update(timestamp);
        synchronized (this) {
            if (this.state == State.WANTED && approvalLatch != null) {
                approvalLatch.countDown();
            }
        }
    }

    @Override
    public synchronized void handleNewPeer(String peerId, String peerIp, int peerPort) throws RemoteException {
        if (peers.containsKey(peerId)) {
            return;
        }

        try {
            Registry remoteRegistry = LocateRegistry.getRegistry(peerIp, peerPort);
            MonitorInterface peerStub = (MonitorInterface) remoteRegistry.lookup("MonitorServer");

            peers.put(peerId, peerStub);
            peerStub.registerPeer(this.name, this);

        } catch (Exception e) {
            System.err.println(String.format("[%s][MESH ERROR] Registration handshake failed with '%s': %s", name, peerId, e.getMessage()));
        }
    }

    @Override
    public synchronized void registerPeer(String peerId, MonitorInterface remoteStub) throws RemoteException {
        if (!peers.containsKey(peerId)) {
            peers.put(peerId, remoteStub);
        }
    }

    public String getServerName() {
        return this.name;
    }

    private boolean timestampIsOlder(int timestamp, String sender) {
        if (this.myRequestTimestamp < timestamp) {
            return true;
        }

        if (this.myRequestTimestamp > timestamp) {
            return false;
        }

        return this.name.compareTo(sender) < 0;
    }

    private void log(String message) {
        System.out.println(String.format("[%s][Clock: %03d] %s", name, clock.getValue(), message));
    }
}
