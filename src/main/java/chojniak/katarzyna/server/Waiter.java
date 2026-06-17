package chojniak.katarzyna.server;

import java.io.Serializable;

public class Waiter implements Comparable<Waiter>, Serializable {
    private static final long serialVersionUID = 1L;

    public final String peerId;
    public final long threadId;
    public final long lamportTimestamp;

    public Waiter(String peerId, long threadId, long lamportTimestamp) {
        this.peerId = peerId;
        this.threadId = threadId;
        this.lamportTimestamp = lamportTimestamp;
    }

    @Override
    public int compareTo(Waiter other) {
        if (this.lamportTimestamp != other.lamportTimestamp) {
            return Long.compare(this.lamportTimestamp, other.lamportTimestamp);
        }

        return this.peerId.compareTo(other.peerId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof Waiter)) return false;
        Waiter other = (Waiter) obj;
        return this.threadId == other.threadId && this.peerId.equals(other.peerId);
    }

    @Override
    public int hashCode() {
        int result = peerId.hashCode();
        result = 31 * result + Long.hashCode(threadId);
        return result;
    }

    @Override
    public String toString() {
        return String.format("Waiter[%s-Thread#%d, ts=%d]", peerId, threadId, lamportTimestamp);
    }
}