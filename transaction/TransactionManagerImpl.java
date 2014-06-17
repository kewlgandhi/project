package transaction;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import transaction.recovery.TMRecoveryManager;
import transaction.TransactionAbortedException;

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
		System.out.println("TM constructor start");
		transactions = new HashMap<Integer, TransactionDetails>();
		// Check for data directory
		checkAndCreateData();
		// Create Log File
		logFile = RMIName + ".log";
		xidCounter = new AtomicInteger(1);
		try {
			recover();
		} catch (FileNotFoundException e) {
			System.out.println("TM: Nothing to recover"+ e.getMessage());
		}
		System.out.println("Creating Log file " + logFile);
		logWriter = new LogWriter(logFile);
		logWriter.loadFile();
		System.out.println("Created LogFile");
		executor = Executors.newSingleThreadExecutor();
		activeTxns = new ConcurrentHashMap<Integer,Object>();
		System.out.println("TM constructor end");
	}

	public void enlist(int xid, ResourceManager rm) throws RemoteException{
		TransactionDetails details = transactions.get(xid);
		StringBuilder logMsg = new StringBuilder("");
		if(details == null){
			details = new TransactionDetails(xid);
			logMsg.append(xid+"@#@").append("Transactions@#@INSERT\n");
		}
		if(!details.rmListContains(rm)){
			details.addToRmList(rm);
			logMsg.append(xid+"@#@").append("Transactions@#@ADD@#@").append(rm.getRMName()).append("\n");
			transactions.put(xid, details);
		}
		executor.submit(new VariableLogger(logMsg.toString(), logWriter));
	}

	public boolean commit(int xid) throws RemoteException,TransactionAbortedException{
		System.out.println("In transaction manager COMMIT");
		TransactionDetails details = transactions.get(xid);
		//System.out.println("Transaction details is " + details.toString());
		StringBuilder logMsg = new StringBuilder("");
		if(details == null){
			//TODO: what to do?
			System.out.println("Details is null");
			throw new TransactionAbortedException(xid, "No details of transaction in the TM");
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
		List<ResourceManager> rmList = details.getRmList();
		boolean allAreReady = true;
		for(ResourceManager rm : rmList){
			try{
				allAreReady = allAreReady && rm.prepare(xid);
				System.out.println(rm.getRMName() + ": " +allAreReady);
			}catch(RemoteException e){
				allAreReady = false;
				abort(xid);
				throw new TransactionAbortedException(xid, "All RMs not ready to commit");
			}
		}
		boolean allCommitted = true;
		if(allAreReady){
			if(dieTMbeforeCommit){
				dieNow();
			}
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
			if(dieTMafterCommit){
				dieNow();
			}
			for(ResourceManager rm : rmList){
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
		for(ResourceManager rm : details.getRmList()){
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

	public State getStatus(int xid) throws RemoteException{
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


	public void setDieTMafterCommit(boolean dieTMafterCommit) throws RemoteException{
		this.dieTMafterCommit = dieTMafterCommit;
	}


	public boolean isDieTMbeforeCommit() {
		return dieTMbeforeCommit;
	}


	public void setDieTMbeforeCommit(boolean dieTMbeforeCommit) throws RemoteException{
		this.dieTMbeforeCommit = dieTMbeforeCommit;
	}


	public int start() {
		int temp;
		System.out.println("TM start started");
		StringBuilder logMsg = new StringBuilder("");
		synchronized (xidCounter) {
			temp = xidCounter.intValue();
			xidCounter.set(xidCounter.get()+1);
			activeTxns.put(temp, new Object());
			transactions.put(temp, new TransactionDetails(temp));
			logMsg.append(temp+"@#@").append("Transactions@#@INSERT\n");
		}
		
		//Logging
		logMsg.append(temp+"@#@").append("START\n");
		Future ret = executor.submit(new TransactionLogger(logMsg.toString(), logWriter));
		try{
			ret.get();
			logWriter.flush();
		}catch(Exception e){
			System.out.println("Error in writing log in 2PC");
		}
		System.out.println("TM start ended");
		//End of logging
		return temp;
	}


	public ConcurrentHashMap<Integer, Object> getActiveTxns() {
		return activeTxns;
	}

	void checkAndCreateData()
	{
		Path path = Paths.get("data");
		if(Files.notExists(path	))
		{
			File dir= new File("data");
			dir.mkdir();
		}
	}

	public void recover() throws FileNotFoundException{
		TMRecoveryManager recoveryManager = new TMRecoveryManager(logFile);
		System.out.println("Recovery Manager instantiated");
		boolean recoveryRequired = recoveryManager.analyze();
		transactions = recoveryManager.getTransactions();
		xidCounter.set(recoveryManager.getMaxXID()+1);

		if(recoveryRequired==false){
			System.out.println("No Need to recover");
			return;
		}
		
		//TODO : should we restart the 2PC for INITIALIZED transactions?
		
		System.out.println("Analyze phase done");
		if(recoveryManager.redo()==false){
			System.out.println("Failed during redo");
			return;
		}
		System.out.println("REDO phase done");
	}
	
}
