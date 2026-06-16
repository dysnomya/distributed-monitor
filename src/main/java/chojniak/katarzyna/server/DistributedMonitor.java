package chojniak.katarzyna.server;

import java.io.Serializable;
import java.util.concurrent.locks.Condition;

public class DistributedMonitor<T extends Serializable> {

    private final DistributedLock lock;
    private final ReplicatedBuffer<T> buffer;

    private final Condition notFull;
    private final Condition notEmpty;

    public DistributedMonitor(DistributedLock lock, ReplicatedBuffer<T> buffer) {
        this.lock = lock;
        this.buffer = buffer;
        this.notFull = lock.newCondition();
        this.notEmpty = lock.newCondition();
    }

    public void put(T value) throws InterruptedException {
        lock.acquire();
        try {
            while (buffer.isFull()) {
                notFull.await();
            }

            buffer.putLocal(value);
            buffer.broadcastPut(value);

            notEmpty.signal();

        } finally {
            lock.release();
        }
    }

    public T get() throws InterruptedException {
        lock.acquire();
        try {
            while (buffer.isEmpty()) {
                notEmpty.await();
            }

            T value = buffer.getLocal();
            buffer.broadcastGet();

            notFull.signal();
            return value;

        } finally {
            lock.release();
        }
    }
}