package chojniak.katarzyna;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CommunicationInterface<T extends Serializable> extends Remote {
    void requestLock(String sender, int timestamp) throws RemoteException;

    void agreeLock(String sender, int timestamp) throws RemoteException;

    void registerPeer(String peerId, CommunicationInterface<T> remoteStub) throws RemoteException;

    void handleNewPeer(String peerId, String peerIp, int peerPort) throws RemoteException;

    void replicatePut(T value) throws RemoteException;

    void replicateGet() throws RemoteException;
}
