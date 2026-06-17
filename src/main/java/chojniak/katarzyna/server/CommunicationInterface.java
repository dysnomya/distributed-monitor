package chojniak.katarzyna.server;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CommunicationInterface extends Remote {

    // Client-to-Server Engine Calls
    void acquireLock() throws InterruptedException, RemoteException;

    void releaseLock() throws RemoteException;

    void registerAndBroadcastWait(String conditionName, long threadId) throws RemoteException;

    void blockLocalThread(long threadId) throws InterruptedException, RemoteException;

    void broadcastSignal(String conditionName) throws RemoteException;

    void broadcastSignalAll(String conditionName) throws RemoteException;

    // Server-to-Server Peer RMI Callbacks
    void receiveLockRequest(String nodeId, long timestamp) throws RemoteException;

    void receiveLockReply(long timestamp) throws RemoteException;

    void receiveWait(String conditionName, String nodeId, long threadId, long lamportTimestamp) throws RemoteException;

    void receiveSignal(String conditionName, String targetNodeId, long targetThreadId) throws RemoteException;

    void registerPeer(String peerName, CommunicationInterface peerStub) throws RemoteException;
}
