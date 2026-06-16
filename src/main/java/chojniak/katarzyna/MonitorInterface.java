package chojniak.katarzyna;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface MonitorInterface<T extends Serializable> extends Remote {
    String getServerName() throws RemoteException;

    void put(T value) throws RemoteException;

    T get() throws RemoteException;

}
