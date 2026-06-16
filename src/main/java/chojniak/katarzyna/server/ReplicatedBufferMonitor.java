package chojniak.katarzyna.server;

import chojniak.katarzyna.CommunicationInterface;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ReplicatedBufferMonitor<T extends Serializable> {

    private final int capacity;

    private final Queue<T> buffer = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();

    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

    private final Set<CommunicationInterface<T>> peers = ConcurrentHashMap.newKeySet();


    public ReplicatedBufferMonitor(int capacity) {
        this.capacity = capacity;
    }

    public void addPeer(CommunicationInterface<T> peer) {
        peers.add(peer);
    }

    public void put(T value) throws RemoteException, InterruptedException {
        lock.lock();
        try {
            while (buffer.size() >= capacity) {
                notFull.await();
            }

            buffer.add(value);
            System.out.println("[BUFFER] put " + value);
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }

        for (CommunicationInterface<T> peer : peers) {
            peer.replicatePut(value);
        }
    }

    public T get() throws RemoteException, InterruptedException {
        T value;
        lock.lock();
        try {
            while (buffer.isEmpty()) {
                notEmpty.await();
            }
            value = buffer.remove();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }

        for (CommunicationInterface<T> peer : peers) {
            peer.replicateGet();
        }

        return value;
    }

    public void replicatePut(T value) {
        lock.lock();
        try {
            buffer.add(value);
            System.out.println("[BUFFER] replicated " + value);
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void replicateGet() {
        lock.lock();
        try {
            buffer.remove();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void waitNotFull() throws InterruptedException {
        lock.lock();
        try {
            while (buffer.size() >= capacity) {
                notFull.await();
            }
        } finally {
            lock.unlock();
        }
    }

    public void waitNotEmpty() throws InterruptedException {
        lock.lock();
        try {
            while (buffer.isEmpty()) {
                notEmpty.await();
            }

        } finally {
            lock.unlock();
        }
    }


    public boolean isFull() {
        lock.lock();
        try {
            return buffer.size() >= capacity;

        } finally {
            lock.unlock();
        }
    }

    public boolean isEmpty() {
        lock.lock();
        try {
            return buffer.isEmpty();

        } finally {
            lock.unlock();
        }
    }

}