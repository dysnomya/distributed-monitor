package chojniak.katarzyna.server;

import java.util.concurrent.atomic.AtomicInteger;

public class LogicalClock {
    private final AtomicInteger clock = new AtomicInteger(0);

    public int increment() {
        return clock.incrementAndGet();
    }

    public int getValue() {
        return clock.get();
    }

    public int update(int incomingTimestamp) {
        while (true) {
            int current = clock.get();
            int next = Math.max(current, incomingTimestamp) + 1;

            if (clock.compareAndSet(current, next)) {
                // break
                return next;
            }
        }
    }
}
