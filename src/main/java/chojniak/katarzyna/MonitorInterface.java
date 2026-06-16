package chojniak.katarzyna;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.UUID;

public interface MonitorInterface extends Remote {
    String getServerName() throws RemoteException;

    void acquire() throws RemoteException;
    void release() throws RemoteException;

    void requestLock(String sender, int timestamp) throws RemoteException;
    void agreeLock(String sender, int timestamp) throws RemoteException;

    void handleNewPeer(String peerId, String peerIp, int peerPort) throws RemoteException;
    void registerPeer(String peerId, MonitorInterface remoteStub) throws RemoteException;
}
