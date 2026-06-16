package chojniak.katarzyna.server;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class BufferMonitor<T> {

    // buffer
    private final int capacity;
    private final Queue<T> buffer = new ArrayDeque<>();

    // locks and conditions
    private final ReentrantLock lock = new ReentrantLock();

    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

    public BufferMonitor(int capacity) {
        this.capacity = capacity;
    }


    public void putLocal(T value) throws InterruptedException {
        lock.lock();
        try {
            while (buffer.size() >= capacity) {
                notFull.await();
            }

            buffer.add(value);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }


    public T getLocal() throws InterruptedException {
        lock.lock();
        try {
            while (buffer.isEmpty()) {
                notEmpty.await();
            }

            T value = buffer.remove();
            notFull.signal();
            return value;
        } finally {
            lock.unlock();
        }
    }

    public void signalNotFull() {
        lock.lock();
        try {
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void signalNotEmpty() {
        lock.lock();
        try {
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public synchronized void awaitNotFull() throws InterruptedException {
        while (buffer.size() >= capacity) {
            wait();
        }
    }


    public synchronized void awaitNotEmpty() throws InterruptedException {
        while (buffer.isEmpty()) {
            wait();
        }
    }
}