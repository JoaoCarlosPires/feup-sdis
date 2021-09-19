import java.rmi.Remote;
import java.rmi.RemoteException;

public interface PeerInterface extends Remote {
    byte[] backup(String fileName, int repDegree) throws RemoteException;
    byte[] restore(String fileName) throws RemoteException;
    byte[] delete(String fileName) throws RemoteException;
    byte[] reclaim(int diskSpace) throws RemoteException;
    byte[] state() throws RemoteException;
}
