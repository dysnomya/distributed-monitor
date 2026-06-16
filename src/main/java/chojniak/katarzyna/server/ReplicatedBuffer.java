package chojniak.katarzyna.server;

import chojniak.katarzyna.CommunicationInterface;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicatedBuffer<T extends Serializable> {

    private final int capacity;
    private final Queue<T> buffer = new ArrayDeque<>();
    private final Set<CommunicationInterface<T>> peers = ConcurrentHashMap.newKeySet();

    public ReplicatedBuffer(int capacity) {
        this.capacity = capacity;
    }

    public void addPeer(CommunicationInterface<T> peer) {
        peers.add(peer);
    }

    public void putLocal(T value) {
        buffer.add(value);
        System.out.println("[BUFFER] put " + value);
    }

    public T getLocal() {
        return buffer.remove();
    }


    public void replicatePut(T value) {
        buffer.add(value);
        System.out.println("[BUFFER] replicated " + value);
    }

    public void replicateGet() {
        buffer.remove();
    }

    public boolean isFull() {
        return buffer.size() >= capacity;
    }

    public boolean isEmpty() {
        return buffer.isEmpty();
    }

    public void broadcastPut(T value) {
        for (CommunicationInterface<T> peer : peers) {
            try {
                peer.replicatePut(value);
            } catch (Exception e) {
                System.err.println("[BUFFER] Failed to replicate put to peer: " + e.getMessage());
            }
        }
    }

    public void broadcastGet() {
        for (CommunicationInterface<T> peer : peers) {
            try {
                peer.replicateGet();
            } catch (Exception e) {
                System.err.println("[BUFFER] Failed to replicate get to peer: " + e.getMessage());
            }
        }
    }
}