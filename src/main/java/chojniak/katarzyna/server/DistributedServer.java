package chojniak.katarzyna.server;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class DistributedServer extends UnicastRemoteObject implements CommunicationInterface {
    private enum LockState {
        RELEASED,
        WANTED,
        HELD
    }

    private final String name;

    private LockState state = LockState.RELEASED;
    private int approvals;
    private long requestTimestamp = Integer.MAX_VALUE;

    private final LogicalClock clock = new LogicalClock();

    private ReentrantLock stateLock = new ReentrantLock();
    private final Condition approvalsReceived = stateLock.newCondition();
    private final Condition stateChanged = stateLock.newCondition();

    private final Map<String, CommunicationInterface> peers = new ConcurrentHashMap<>();
    private final Set<String> deferred = ConcurrentHashMap.newKeySet();

    private final Map<String, PriorityQueue<Waiter>> globalConditionQueues = new ConcurrentHashMap<>();
    private final Map<Long, Condition> localThreadConditions = new ConcurrentHashMap<>();

    public DistributedServer(String name) throws RemoteException {
        super();
        this.name = name;
    }

    @Override
    public void acquireLock() throws InterruptedException {
        long timestamp;

        stateLock.lock();
        try {
            while (state != LockState.RELEASED) {
                stateChanged.await();
            }

            state = LockState.WANTED;
            timestamp = requestTimestamp = clock.increment();

            approvals = peers.size();
        } finally {
            stateLock.unlock();
        }

        sendRequests(timestamp);

        stateLock.lock();
        try {
            while (approvals > 0) {
                approvalsReceived.await();
            }
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public void releaseLock() {
        Set<String> waiting;

        stateLock.lock();
        try {
            state = LockState.RELEASED;
            stateChanged.signalAll();

            requestTimestamp = Integer.MAX_VALUE;
            waiting = Set.copyOf(deferred);
            deferred.clear();
        } finally {
            stateLock.unlock();
        }

        waiting.forEach(this::sendReply);
    }

    @Override
    public void registerAndBroadcastWait(String conditionName, long threadId) {
        long timestamp;

        stateLock.lock();
        try {
            timestamp = clock.increment();
            Waiter waiter = new Waiter(name, threadId, timestamp);
            globalConditionQueues.computeIfAbsent(conditionName, k -> new PriorityQueue<>()).add(waiter);
            localThreadConditions.computeIfAbsent(threadId, t -> stateLock.newCondition());
            System.out.println("priority queue size: " + globalConditionQueues.size());
            System.out.println("[WAIT] Registered remote waiter: " + waiter + " on condition: " + conditionName);

        } finally {
            stateLock.unlock();
        }

        peers.values().forEach(peer -> {
            try {
                peer.receiveWait(conditionName, name, threadId, timestamp);
            } catch (RemoteException ignored) {
            }
        });
    }

    @Override
    public void blockLocalThread(long threadId) throws InterruptedException {
        stateLock.lock();
        try {
            while (localThreadConditions.containsKey(threadId)) {
                Condition localCondition = localThreadConditions.get(threadId);
                if (localCondition != null) {
                    localCondition.await();
                }
            }
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public void broadcastSignal(String conditionName) {
        Waiter target = null;

        stateLock.lock();
        try {
            PriorityQueue<Waiter> queue = globalConditionQueues.get(conditionName);
            if (queue != null && !queue.isEmpty()) {
                target = queue.poll();
            }
        } finally {
            stateLock.unlock();
        }

        if (target != null) {
            try {
                for (CommunicationInterface peer : peers.values()) {
                    peer.receiveSignal(conditionName, target.peerId, target.threadId);
                }
            } catch (RemoteException ignored) {
            }

            try {
                receiveSignal(conditionName, target.peerId, target.threadId);
            } catch (RemoteException ignored) {
            }
        }
    }

    @Override
    public void broadcastSignalAll(String conditionName) {
        List<Waiter> targets = new ArrayList<>();

        stateLock.lock();
        try {
            PriorityQueue<Waiter> queue = globalConditionQueues.get(conditionName);
            if (queue != null) {
                while (!queue.isEmpty()) {
                    targets.add(queue.poll());
                }
            }
        } finally {
            stateLock.unlock();
        }

        if (!targets.isEmpty()) {
            for (Waiter target : targets) {
                peers.values().forEach(peer -> {
                    try {
                        peer.receiveSignal(conditionName, target.peerId, target.threadId);
                    } catch (RemoteException ignored) {
                    }
                });

                try {
                    receiveSignal(conditionName, target.peerId, target.threadId);
                } catch (RemoteException ignored) {
                }
            }
        }
    }

    // --- RMI Remote Methods (Called by Peer Nodes) ---
    @Override
    public void receiveLockRequest(String remoteNodeId, long timestamp) throws RemoteException {
        System.out.println("[RECEIVE] request from " + remoteNodeId);
        clock.update(timestamp);
        boolean shouldDefer = false;

        stateLock.lock();
        try {
            if (state == LockState.HELD || (state == LockState.WANTED && hasPriority(timestamp, remoteNodeId))) {
                deferred.add(remoteNodeId);
                shouldDefer = true;
            }
        } finally {
            stateLock.unlock();
        }

        if (!shouldDefer) {
            sendReply(remoteNodeId);
        }
    }

    @Override
    public void receiveLockReply(long timestamp) throws RemoteException {
        System.out.println("[RECEIVE] reply");
        clock.update(timestamp);

        stateLock.lock();
        try {
            if (state == LockState.WANTED && approvals > 0) {
                approvals--;
                if (approvals == 0) {
                    approvalsReceived.signal();
                }
            }
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public void receiveWait(String conditionName, String peerId, long threadId, long timestamp) throws RemoteException {
        clock.update(timestamp);

        stateLock.lock();
        try {
            Waiter remoteWaiter = new Waiter(peerId, threadId, timestamp);
            globalConditionQueues.computeIfAbsent(conditionName, k -> new PriorityQueue<>()).add(remoteWaiter);
            System.out.println("[WAIT] Registered remote waiter: " + remoteWaiter + " on condition: " + conditionName);
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public void receiveSignal(String conditionName, String peerId, long targetThreadId) throws RemoteException {
        stateLock.lock();
        try {
            PriorityQueue<Waiter> queue = globalConditionQueues.get(conditionName);
            if (queue != null) {
                queue.removeIf(waiter -> waiter.peerId.equals(peerId) && waiter.threadId == targetThreadId);
            }

            if (peerId.equals(this.name)) {
                Condition localCondition = localThreadConditions.remove(targetThreadId);

                if (localCondition != null) {
                    localCondition.signal();
                }
            }

        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public void registerPeer(String peerName, CommunicationInterface peerStub) throws RemoteException {
        peers.putIfAbsent(peerName, peerStub);
        System.out.println("[CLUSTER] Peer successfully linked: " + peerName);
    }

    public void handleNewPeer(String peerId, String peerIp, int peerPort) {
        try {
            Registry registry = LocateRegistry.getRegistry(peerIp, peerPort);
            CommunicationInterface peerStub = (CommunicationInterface) registry.lookup("MonitorServer");

            registerPeer(peerId, peerStub);
            peerStub.registerPeer(this.name, this);

        } catch (Exception e) {
            System.err.println("[CLUSTER] Failed to connect to bootstrapper peer: " + e);
        }

    }

    private void sendRequests(long timestamp) {
        peers.forEach((id, peer) -> {
            try {
                peer.receiveLockRequest(name, timestamp);
                System.out.println("[SEND] sent to " + id);
            } catch (Exception e) {
                // PEER FAILED
                stateLock.lock();
                try {
                    if (state == LockState.WANTED && approvals > 0) {
                        approvals--;
                        if (approvals == 0) {
                            approvalsReceived.signal();
                        }
                    }
                } finally {
                    stateLock.unlock();
                }
            }
        });
    }

    private void sendReply(String peerId) {
        System.out.println("[SEND] reply to " + peerId);
        CommunicationInterface peer = peers.get(peerId);
        if (peer == null) return;

        try {
            peer.receiveLockReply(clock.increment());
        } catch (RemoteException ignored) {
            System.out.println("an error here");
        }
    }

    private boolean hasPriority(long remoteTimestamp, String senderId) {
        if (this.requestTimestamp < remoteTimestamp) {
            return true;
        }
        if (this.requestTimestamp == remoteTimestamp) {
            return this.name.compareTo(senderId) < 0;
        }
        return false;
    }


}
