package chojniak.katarzyna.server;

import chojniak.katarzyna.CommunicationInterface;
import chojniak.katarzyna.State;

import java.rmi.RemoteException;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class DistributedLock {

    private final String name;
    private final LogicalClock clock = new LogicalClock();

    private final ReentrantLock stateLock = new ReentrantLock();
    private final Condition approvalsReceived = stateLock.newCondition();

    private final Map<String, CommunicationInterface> peers = new ConcurrentHashMap<>();

    private final Set<String> deferred = ConcurrentHashMap.newKeySet();


    private State state = State.RELEASED;
    private int requestTimestamp;
    private int approvals;

    public DistributedLock(String name) {
        this.name = name;
    }

    public Condition newCondition() {
        return new DistributedCondition();
    }

    public void acquire() throws InterruptedException {
        int timestamp;

        stateLock.lock();
        try {
            state = State.WANTED;
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
            state = State.HELD;

        } finally {
            stateLock.unlock();
        }
    }

    public void release() {
        Set<String> waiting;

        stateLock.lock();
        try {
            state = State.RELEASED;
            requestTimestamp = Integer.MAX_VALUE;

            waiting = Set.copyOf(deferred);
            deferred.clear();

        } finally {
            stateLock.unlock();
        }

        waiting.forEach(this::grant);
    }

    public void request(String sender, int timestamp) {
        clock.update(timestamp);

        stateLock.lock();
        try {
            if (state == State.HELD || (state == State.WANTED && hasPriority(timestamp, sender))) {
                deferred.add(sender);
                return;
            }
        } finally {
            stateLock.unlock();
        }
        grant(sender);
    }

    public void agree(String sender, int timestamp) {
        clock.update(timestamp);

        stateLock.lock();
        try {
            approvals--;
            if (approvals == 0) {
                approvalsReceived.signal();
            }
        } finally {
            stateLock.unlock();
        }
    }

    private void sendRequests(int timestamp) {
        peers.forEach((id, peer) -> {
            try {
                peer.requestLock(name, timestamp);
            } catch (Exception e) {
                stateLock.lock();
                try {
                    approvals--;
                } finally {
                    stateLock.unlock();
                }
            }
        });
    }

    private void grant(String peerId) {
        CommunicationInterface peer = peers.get(peerId);
        if (peer == null) {
            return;
        }

        try {
            peer.agreeLock(name, clock.increment());
        } catch (RemoteException ignored) {
        }
    }

    private boolean hasPriority(int timestamp, String sender) {
        return requestTimestamp < timestamp || (requestTimestamp == timestamp && name.compareTo(sender) < 0);
    }

    public void addPeer(String id, CommunicationInterface peer) {
        peers.putIfAbsent(id, peer);
    }

    private class DistributedCondition implements Condition {

        private final Condition condition = stateLock.newCondition();

        private void releaseForWait() {
            DistributedLock.this.release();
        }


        private void reacquireAfterWait() throws InterruptedException {
            DistributedLock.this.acquire();
        }


        @Override
        public void await() throws InterruptedException {
            releaseForWait();

            stateLock.lock();
            try {
                condition.await();
            } finally {
                stateLock.unlock();
            }

            reacquireAfterWait();
        }


        @Override
        public void awaitUninterruptibly() {
            releaseForWait();

            stateLock.lock();
            try {
                condition.awaitUninterruptibly();
            } finally {
                stateLock.unlock();
            }

            boolean interrupted = false;

            while (true) {
                try {
                    reacquireAfterWait();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }


        @Override
        public long awaitNanos(long nanosTimeout) throws InterruptedException {
            releaseForWait();
            long remaining;
            stateLock.lock();
            try {
                remaining = condition.awaitNanos(nanosTimeout);
            } finally {
                stateLock.unlock();
            }
            reacquireAfterWait();
            return remaining;
        }


        @Override
        public boolean await(long time, TimeUnit unit) throws InterruptedException {
            releaseForWait();
            boolean result;

            stateLock.lock();
            try {
                result = condition.await(time, unit);
            } finally {
                stateLock.unlock();
            }

            reacquireAfterWait();

            return result;
        }


        @Override
        public boolean awaitUntil(Date deadline) throws InterruptedException {
            releaseForWait();
            boolean result;

            stateLock.lock();
            try {
                result = condition.awaitUntil(deadline);
            } finally {
                stateLock.unlock();
            }

            reacquireAfterWait();
            return result;
        }


        @Override
        public void signal() {
            stateLock.lock();

            try {
                condition.signal();
            } finally {
                stateLock.unlock();
            }
        }


        @Override
        public void signalAll() {
            stateLock.lock();

            try {
                condition.signalAll();
            } finally {
                stateLock.unlock();
            }
        }
    }
}