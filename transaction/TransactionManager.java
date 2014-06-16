package transaction;

import java.rmi.*;
import java.util.concurrent.ConcurrentHashMap;

/** 
 * Interface for the Transaction Manager of the Distributed Travel
 * Reservation System.
 * <p>
 * Unlike WorkflowController.java, you are supposed to make changes
 * to this file.
 */

public interface TransactionManager extends Remote {

    public boolean dieNow()
	throws RemoteException;


    /** The RMI name a TransactionManager binds to. */
    public static final String RMIName = "TM";


	public void enlist(int xid, ResourceManager resourceManager) throws RemoteException;


	public ConcurrentHashMap<Integer, Object> getActiveTxns() throws RemoteException;
	public int start() throws RemoteException;
	public boolean commit(int xid) throws RemoteException;
	public boolean abort(int xid) throws RemoteException;
	public void setDieTMafterCommit(boolean b) throws RemoteException;
	public void setDieTMbeforeCommit(boolean b) throws RemoteException;

}
