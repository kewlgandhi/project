package transaction;

import java.rmi.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import transaction.bean.State;
import transaction.bean.TransactionDetails;
import transaction.logmgr.LogWriter;
import transaction.logmgr.TransactionLogger;
import transaction.logmgr.VariableLogger;

/** 
 * Transaction Manager for the Distributed Travel Reservation System.
 * 
 * Description: toy implementation of the TM
 */

public class TransactionManagerImpl extends java.rmi.server.UnicastRemoteObject implements TransactionManager {
	
	private Map<Integer, TransactionDetails> transactions;
	protected AtomicInteger xidCounter;
	private LogWriter logWriter;
	private String logFile;
	private ExecutorService executor;
	private boolean dieTMbeforeCommit = false;
	private boolean dieTMafterCommit = false;
	private ConcurrentHashMap<Integer,Object> activeTxns;
	
	public static void main(String args[]) {
		System.setSecurityManager(new RMISecurityManager());

		String rmiPort = System.getProperty("rmiPort");
		if (rmiPort == null) {
			rmiPort = "";
		} else if (!rmiPort.equals("")) {
			rmiPort = "//:" + rmiPort + "/";
		}

		try {
			TransactionManagerImpl obj = new TransactionManagerImpl();
			Naming.rebind(rmiPort + TransactionManager.RMIName, obj);
			System.out.println("TM bound");
		} 
		catch (Exception e) {
			System.err.println("TM not bound:" + e);
			System.exit(1);
		}
	}


	public TransactionManagerImpl() throws RemoteException {
		transactions = new HashMap<Integer, TransactionDetails>();
		// Create Log File
		logFile = RMIName + ".log";
		logWriter = new LogWriter(logFile);
		executor = Executors.newSingleThreadExecutor();
		activeTxns = new ConcurrentHashMap<Integer,Object>();
		xidCounter = new AtomicInteger(0);
	}
	
	public void enlist(int xid, ResourceManagerImpl rm){
		TransactionDetails details = transactions.get(xid);
		StringBuilder logMsg = new StringBuilder("");
		if(details == null){
			details = new TransactionDetails(xid);
			logMsg.append(xid+"@#@").append("Transactions@#@INSERT\n");
		}
		if(!details.rmListContains(rm)){
			details.addToRmList(rm);
			logMsg.append(xid+"@#@").append("Transactions@#@ADD@#@").append(rm.myRMIName).append("\n");
			transactions.put(xid, details);
		}
		executor.submit(new VariableLogger(logMsg.toString(), logWriter));
	}
	
	public boolean commit(int xid){
		TransactionDetails details = transactions.get(xid);
		StringBuilder logMsg = new StringBuilder("");
		if(details == null){
			//TODO: what to do?
		}
		details.setStatus(State.INITIALIZED);
		//Logging
		logMsg.append(xid+"@#@").append("Transactions@#@STATUS@#@").append(State.INITIALIZED.toString()+"\n");
		Future ret = executor.submit(new TransactionLogger(logMsg.toString(), logWriter));
		try{
			ret.get();
			logWriter.flush();
		}catch(Exception e){
			System.out.println("Error in writing log in 2PC");
		}
		logMsg = new StringBuilder("");
		//End of Logging
		List<ResourceManagerImpl> rmList = details.getRmList();
		boolean allAreReady = true;
		for(ResourceManagerImpl rm : rmList){
			try{
				allAreReady = allAreReady && rm.prepare(xid);
			}catch(RemoteException e){
				allAreReady = false;
			}
		}
		boolean allCommitted = true;
		if(allAreReady){
			details.setStatus(State.PREPARED);
			//Logging
			logMsg.append(xid+"@#@").append("Transactions@#@STATUS@#@").append(State.PREPARED.toString()+"\n");
			ret = executor.submit(new TransactionLogger(logMsg.toString(), logWriter));
			try{
				ret.get();
				logWriter.flush();
			}catch(Exception e){
				System.out.println("Error in writing log in 2PC");
			}
			logMsg = new StringBuilder("");
			//End of Logging
			for(ResourceManagerImpl rm : rmList){
				try{
					allCommitted = allCommitted && rm.commit(xid);
				}catch(RemoteException e){
					allCommitted = false;
				}catch(Exception ex){
					System.out.println("Decide what to do");
					//TODO: decide what to do
				}
			}
			if(allCommitted){
				details.setStatus(State.COMMITTED);
				transactions.remove(xid);
				//Logging
				logMsg.append(xid+"@#@").append("Transactions@#@STATUS@#@").append(State.COMMITTED.toString()+"\n");
				logMsg.append(xid+"@#@").append("Transactions@#@DELETE@#@").append("\n");
				ret = executor.submit(new TransactionLogger(logMsg.toString(), logWriter));
				try{
					ret.get();
					logWriter.flush();
				}catch(Exception e){
					System.out.println("Error in writing log in 2PC");
				}
				logMsg = new StringBuilder("");
				//End of Logging
				
			}
			
		}else{
			abort(xid);
			return false;
		}
		activeTxns.remove(xid);
		return true;
	}
	
	public boolean abort(int xid){
		TransactionDetails details = transactions.get(xid);
		StringBuilder logMsg = new StringBuilder("");
		if(details == null){
			//TODO: what to do?
		}
		details.setStatus(State.ABORTED);
		transactions.remove(xid);
		//Logging
		logMsg.append(xid+"@#@").append("Transactions@#@STATUS@#@").append(State.ABORTED.toString()+"\n");
		logMsg.append(xid+"@#@").append("Transactions@#@DELETE@#@").append("\n");
		Future ret = executor.submit(new TransactionLogger(logMsg.toString(), logWriter));
		try{
			ret.get();
			logWriter.flush();
		}catch(Exception e){
			System.out.println("Error in writing log in 2PC");
		}
		logMsg = new StringBuilder("");
		//End of Logging
		boolean allAborted = true;
		for(ResourceManagerImpl rm : details.getRmList()){
			try{
				rm.abort(xid);
			}catch(RemoteException e){
				allAborted = false;
			}catch(Exception ex){
				System.out.println("Decide what to do");
				//TODO: decide what to do
			}
		}
		
		activeTxns.remove(xid);
		if(!allAborted){
			return false;
		}
		return true;
	}
	
	public State getStatus(int xid){
		TransactionDetails details = transactions.get(xid);
		if(details == null){
			return State.ABORTED;
		}
		return details.getStatus();
	}

	public boolean dieNow() 
			throws RemoteException {
		System.exit(1);
		return true; // We won't ever get here since we exited above;
		// but we still need it to please the compiler.
	}


	public boolean isDieTMafterCommit() {
		return dieTMafterCommit;
	}


	public void setDieTMafterCommit(boolean dieTMafterCommit) {
		this.dieTMafterCommit = dieTMafterCommit;
	}


	public boolean isDieTMbeforeCommit() {
		return dieTMbeforeCommit;
	}


	public void setDieTMbeforeCommit(boolean dieTMbeforeCommit) {
		this.dieTMbeforeCommit = dieTMbeforeCommit;
	}


	public int start() {
		int temp;
		synchronized (xidCounter) {
			temp = xidCounter.intValue();
			xidCounter.set(xidCounter.get()+1);
			activeTxns.put(temp, new Object());
		}
		StringBuilder logMsg = new StringBuilder("");
		//Logging
		logMsg.append(temp+"@#@").append("START\n");
		Future ret = executor.submit(new TransactionLogger(logMsg.toString(), logWriter));
		try{
			ret.get();
			logWriter.flush();
		}catch(Exception e){
			System.out.println("Error in writing log in 2PC");
		}
		//End of logging
		return temp;
	}


	public ConcurrentHashMap<Integer, Object> getActiveTxns() {
		return activeTxns;
	}
	
}
