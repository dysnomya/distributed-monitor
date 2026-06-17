package chojniak.katarzyna.client;

import java.rmi.RemoteException;

public interface DistributedMonitor {
    void lock() throws RemoteException;

    void unlock() throws RemoteException;

    DistributedCondition newCondition(String conditionName);

    interface DistributedCondition {
        void await() throws InterruptedException, RemoteException;

        void signal() throws RemoteException;

        void signalAll() throws RemoteException;
    }
}