package distributedbackup.service;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface InitiatorPeer extends Remote {
    void backup(String pathname, int replicationDegree) throws RemoteException;

    void restore(String pathname) throws RemoteException;

    void delete(String pathname) throws RemoteException;

    void reclaim(int maxDiskSpace) throws RemoteException;

    void getState() throws RemoteException;
}
