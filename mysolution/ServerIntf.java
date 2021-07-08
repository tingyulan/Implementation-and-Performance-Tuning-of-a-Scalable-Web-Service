import java.rmi.Remote;
import java.io.*;
import java.rmi.RemoteException;

public interface ServerIntf extends Remote {
    public int getOperation(int VM_id) throws RemoteException;
    public void add(Cloud.FrontEndOps.Request r) throws RemoteException;
	public Cloud.FrontEndOps.Request poll() throws RemoteException;
    public int getNumMiddle() throws RemoteException;
    public void middleScaleIn(int id) throws RemoteException;
    public Cloud.DatabaseOps getCache() throws RemoteException;
}
