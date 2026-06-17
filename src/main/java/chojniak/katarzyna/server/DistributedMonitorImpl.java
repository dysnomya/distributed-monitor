package chojniak.katarzyna.server;

import chojniak.katarzyna.client.DistributedMonitor;

import java.rmi.RemoteException;
import java.util.Random;

public class DistributedMonitorImpl implements DistributedMonitor {

    private final CommunicationInterface server;

    public DistributedMonitorImpl(CommunicationInterface server) throws RemoteException {
        super();
        this.server = server;
    }

    @Override
    public void lock() throws RemoteException {
        try {
            server.acquireLock();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteException("Lock acquisition thread was interrupted", e);
        }
    }

    @Override
    public void unlock() throws RemoteException {
        server.releaseLock();
    }

    @Override
    public DistributedCondition newCondition(String conditionName) {
        return new DistributedConditionImpl(conditionName);
    }

    private class DistributedConditionImpl implements DistributedMonitor.DistributedCondition {
        private final String conditionName;

        public DistributedConditionImpl(String conditionName) {
            this.conditionName = conditionName;
        }

        @Override
        public void await() throws InterruptedException, RemoteException {
//            long threadId = Thread.currentThread().threadId();
            long threadId = new Random().nextLong();

            server.registerAndBroadcastWait(conditionName, threadId);
            server.releaseLock();
            server.blockLocalThread(threadId);
            server.acquireLock();
        }

        @Override
        public void signal() throws RemoteException {
            server.broadcastSignal(conditionName);
        }

        @Override
        public void signalAll() throws RemoteException {
            server.broadcastSignalAll(conditionName);
        }
    }
}
