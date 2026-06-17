package chojniak.katarzyna.server;

import java.util.concurrent.atomic.AtomicLong;

public class LogicalClock {
    private final AtomicLong clock = new AtomicLong(0);

    public long increment() {
        return clock.incrementAndGet();
    }

    public long getValue() {
        return clock.get();
    }

    public long update(long incomingTimestamp) {
        while (true) {
            long current = clock.get();
            long next = Math.max(current, incomingTimestamp) + 1;

            if (clock.compareAndSet(current, next)) {
                // break
                return next;
            }
        }
    }
}
