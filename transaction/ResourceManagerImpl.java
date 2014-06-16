package transaction;

import lockmgr.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import transaction.bean.Car;
import transaction.bean.Flight;
import transaction.bean.Hotels;
import transaction.bean.Reservation;
import transaction.bean.TableWriter;
import transaction.bean.UndoIMLog;
import transaction.logmgr.LogWriter;
import transaction.logmgr.TransactionLogger;
import transaction.logmgr.VariableLogger;
import transaction.recovery.LoadFiles;
import transaction.recovery.RecoveryManager;

/** 
 * Resource Manager for the Distributed Travel Reservation System.
 * 
 * Description: toy implementation of the RM
 */

public class ResourceManagerImpl
extends java.rmi.server.UnicastRemoteObject
implements ResourceManager {

	protected String myRMIName = null; // Used to distinguish this RM from other RMs
	protected TransactionManager tm = null;

	// Constants
	// Other Variables
	private final Object DUMMY = new Object();
	private final int WRITE = 1;
	private final int READ = 0;
	private final int CHECKPOINT_TRIGGER = 10;
	private final int SLEEPSHUTDOWN = 5000;

	//Die now test variables
	private boolean dieAfterEnlist = false;
	private boolean dieBeforePrepare = false;
	public void setDieAfterEnlist(boolean dieAfterEnlist) throws RemoteException{
		this.dieAfterEnlist = dieAfterEnlist;
	}

	public void setDieBeforePrepare(boolean dieBeforePrepare) throws RemoteException{
		this.dieBeforePrepare = dieBeforePrepare;
	}

	public void setDieAfterPrepare(boolean dieAfterPrepare) throws RemoteException{
		this.dieAfterPrepare = dieAfterPrepare;
	}

	public void setDieBeforeCommit(boolean dieBeforeCommit) throws RemoteException{
		this.dieBeforeCommit = dieBeforeCommit;
	}

	public void setDieBeforeAbort(boolean dieBeforeAbort) throws RemoteException{
		this.dieBeforeAbort = dieBeforeAbort;
	}

	private boolean dieAfterPrepare = false;
	private boolean dieBeforeCommit = false;
	private boolean dieBeforeAbort = false;

	// Data Sets
	private ConcurrentHashMap<String,Flight> flightTable;
	private ConcurrentHashMap<String,Car> carTable;
	private ConcurrentHashMap<String,Hotels> hotelTable;
	private ConcurrentHashMap<String,HashSet<Reservation>> reservationTable;
	private ConcurrentHashMap<String,Integer> reservedflights;

	//Book keeping and other variables
	private LockManager lockManager;

	protected int xidCounter;

	private ConcurrentHashMap<Integer,Object> activeTxns;
	private ConcurrentHashMap<Integer, Object> abrtdTxns;
	private ConcurrentHashMap<Integer, Object> comtdTxns;
	private ConcurrentHashMap<Integer, Object> prprdTxns;

	private volatile AtomicInteger shuttingDown = new AtomicInteger();
	private volatile AtomicInteger committedTrxns = new  AtomicInteger();
	private volatile Integer enteredTxnsCount=0;
	private AtomicBoolean stopAndWait = new AtomicBoolean(false);
	private AtomicBoolean HashSetEmpty = new AtomicBoolean(true);

	private Boolean dieBeforePointerSwitch = new Boolean(false);
	private Boolean dieAfterPointerSwitch = new Boolean(false);

	private ExecutorService checkPointers ;
	private Set<Callable<Integer>> callables;

	private ExecutorService executor ;
	private LogWriter logWriter;
	private String logFile;

	//<----------UNDOING--------------------->
	private ConcurrentHashMap<Integer,Stack<UndoIMLog> > UndoIMTable;

	private static final int empty				= 0;
	private static final int FlightTable  		= 1;
	private static final int CarTable 			= 2;
	private static final int HotelTable 		= 3;
	private static final int ReservationTable 	= 4;
	private static final int Flights 			= 5;

	private static final int insert 			= 1;
	private static final int delete				= 2;
	private static final int overWrite			= 3;
	private static final int partialInsert 		= 4;
	//</----------UNDOING--------------------->


	public static void main(String args[]) {
		System.setSecurityManager(new RMISecurityManager());

		String rmiName = System.getProperty("rmiName");
		if (rmiName == null || rmiName.equals("")) {
			System.err.println("No RMI name given");
			System.exit(1);
		}

		String rmiPort = System.getProperty("rmiPort");
		if (rmiPort == null) {
			rmiPort = "";
		} else if (!rmiPort.equals("")) {
			rmiPort = "//:" + rmiPort + "/";
		}

		try {
			ResourceManagerImpl obj = new ResourceManagerImpl(rmiName);
			Naming.rebind(rmiPort + rmiName, obj);
			System.out.println(rmiName + " bound");
		} 
		catch (Exception e) {
			System.err.println(rmiName + " not bound:" + e);
			System.exit(1);
		}
	}

	//TODO Should delegate this to WC ?
	void checkAndCreateData()
	{
		Path path = Paths.get("data");
		if(Files.notExists(path	))
		{
			File dir= new File("data");
			dir.mkdir();
		}
	}

	public ResourceManagerImpl(String rmiName) throws RemoteException {
		myRMIName = rmiName;
		while (!reconnect()) {
			// would be better to sleep a while
		} 

		System.out.println("Starting Constructor: "+ myRMIName);
		lockManager = new LockManager();

		activeTxns = new ConcurrentHashMap<Integer,Object>();
		abrtdTxns = new ConcurrentHashMap<Integer, Object>();
		comtdTxns = new ConcurrentHashMap<Integer, Object>();
		prprdTxns = new ConcurrentHashMap<Integer, Object>();

		flightTable = new ConcurrentHashMap<String, Flight>();
		carTable = new ConcurrentHashMap<String, Car>();
		hotelTable = new ConcurrentHashMap<String, Hotels>();
		reservationTable = new ConcurrentHashMap<String, HashSet<Reservation>>();
		reservedflights = new ConcurrentHashMap<String,Integer>();

		executor = Executors.newSingleThreadExecutor();

		//<----------UNDOING--------------------->
		UndoIMTable = new ConcurrentHashMap<Integer,Stack<UndoIMLog>>();
		//</----------UNDOING--------------------->

		// For writing data to disk
		callables = new HashSet<Callable<Integer>>();
		callables.add(new TableWriter((Object)flightTable,"flightTable"));
		callables.add(new TableWriter((Object)carTable,"carTable"));
		callables.add(new TableWriter((Object)hotelTable,"hotelTable"));
		callables.add(new TableWriter((Object)reservationTable,"reservationTable"));
		callables.add(new TableWriter((Object)reservedflights,"reservedFlights"));
		checkPointers = Executors.newFixedThreadPool(5); 

		// Check for data directory
		checkAndCreateData();

		// Name the Log File
		logFile = myRMIName + ".log";
		


		try{
			loadFiles();
		}
		catch(FileNotFoundException e){
			System.out.println("Cannot find file/s" + e.getMessage());
		}

		System.out.println("Load files done/not done: " + myRMIName);		
		try{
			recover();
		}
		catch(FileNotFoundException e){
			System.out.println("Nothing to recover"+ e.getMessage());
		}
		System.out.println("Recovery done: " + myRMIName);
		//create the log file
		logWriter = new LogWriter(logFile);
		logWriter.loadFile();
		System.out.println("Closing constructor: "+ myRMIName);
	}

	public void isRegistered(int xid)
			throws InvalidTransactionException, TransactionAbortedException, RemoteException{
		System.out.println("ResourceManager: isRegistered started");
		if(abrtdTxns.containsKey(xid)){
			throw new TransactionAbortedException(xid,"isRegistered: "+ myRMIName );
		}
		System.out.println("ResourceManager: Done checking in abrtdTxns");
		if(comtdTxns.containsKey(xid)){
			System.out.println("Should Not Reach Here, isRegistered: "+myRMIName);
			throw new InvalidTransactionException(xid, "");
		}
		System.out.println("ResourceManager: Done checking in comtdTxns");
		if(activeTxns.containsKey(xid))
			return;
		System.out.println("ResourceManager: Done checking in activeTxns");
		start(xid);
		System.out.println("ResourceManager: isRegistered ended");
		return;

	}

	//	public void isValidTrxn(int xid)
	//	throws InvalidTransactionException, TransactionAbortedException{
	//if(abrtdTxns.contains(xid)){
	//	throw new TransactionAbortedException(xid,"isValidTrxn: "+ myRMIName );
	//}
	//if(activeTxns.get(xid) == null){
	//	throw new InvalidTransactionException(xid,"isValidTrxn: "+ myRMIName);
	//}
	//return ;
	//
	//}

	private void updateCheckPointVariables()
	{
		// If there is no shutdown , then check pointing is over, 
		// this means we can reset the stopAndWait flag.
		committedTrxns.set(0);
		synchronized(HashSetEmpty){
			HashSetEmpty.set(true);
		}
		synchronized(enteredTxnsCount){
			enteredTxnsCount=0;
		}
		synchronized(stopAndWait){
			stopAndWait.set(false);
			stopAndWait.notifyAll();
		}
	}

	private void checkPoint (int tries) throws RemoteException
	{
		System.out.println("CHeckpoint begin");
		boolean failed=false;
		try
		{
			List<Future<Integer>> futures = checkPointers.invokeAll(callables);


			if(tries==3){
				System.out.println("CHECKPOINTING FAILED THRICE: KILLING SYSTEM: "+ myRMIName);
				dieNow();
			}

			for(Future<Integer> future : futures){
				if(future.get()==1)
				{
					System.out.println(tries+ " CHECKPOINTING FAILED: "+ myRMIName);
					failed=true;
				}
			}

		}
		catch(InterruptedException e){
			e.printStackTrace();
		}
		catch(ExecutionException e){
			e.printStackTrace();
		}
		if(failed)
			checkPoint(tries+1);
		logWriter.flush();
		System.out.println("Checkpoint END: "+ myRMIName);
		return;
	}

	private void stopIncoming() throws RemoteException
	{
		synchronized(stopAndWait)
		{
			if(!stopAndWait.get()){
				stopAndWait.set(true);
			}
			else
				// 	means stopandwait already raised due to some other condition.
				// 	makes no difference , can only happen in case of shutdown/Cp or CP/shutdown.
				// 	hence do nothing.
				return;
		}
		//wait for all transactions to get over. Sleep on the HashSetEmpty object.
		synchronized(HashSetEmpty){
			while(!HashSetEmpty.get())
			{
				try{
					HashSetEmpty.wait();
				}
				catch(InterruptedException e)
				{
					System.out.println(" value of HashSetEmpty"+HashSetEmpty.toString());
					e.printStackTrace();
				}
			}
		}

		//do a checkpoint always. if shutdown flag is enabled, then also shutdown the system.
		//code for check pointing

		checkPoint(0);
		RecoveryManager recoveryManager = new RecoveryManager(logFile);
		try{
			System.out.println("Deleting logs: "+ myRMIName);
			recoveryManager.deleteLogs();
			System.out.println("Deleted logs: "+ myRMIName);
		}
		catch(SecurityException e){
			System.out.println("Security permission issues: "+e.getMessage());
		}
		catch(FileNotFoundException e){
			System.out.println("Could not find logs while trying to delete them: "+ myRMIName);
		}
		catch(IllegalMonitorStateException e){
			e.printStackTrace();
			e.getCause();
		}
		if(shuttingDown.get()>0)
			System.exit(0);
		System.out.println("About to update check point variables: "+ myRMIName);
		updateCheckPointVariables();
		System.out.println("Stop incoming ends: "+ myRMIName);
		return;
	}


	public boolean reconnect()
			throws RemoteException {
		String rmiPort = System.getProperty("rmiPort");
		if (rmiPort == null) {
			rmiPort = "";
		} else if (!rmiPort.equals("")) {
			rmiPort = "//:" + rmiPort + "/";
		}

		try {
			tm = (TransactionManager)Naming.lookup(rmiPort + TransactionManager.RMIName);
			System.out.println(myRMIName + " bound to TM");
		} 
		catch (Exception e) {
			System.err.println(myRMIName + " cannot bind to TM:" + e);
			return false;
		}

		return true;
	}

	// TRANSACTION INTERFACE
	public void start(int xid) throws RemoteException {	
		//Register this RM in TM
		tm.enlist(xid,this);
		if(dieAfterEnlist){
			dieNow();
		}

		synchronized(enteredTxnsCount){
			System.out.println("Entering start: "+ myRMIName);
			synchronized(stopAndWait){
				while(stopAndWait.get()){
					try{
						System.out.println("Waiting on stopAndWait: "+ myRMIName);
						stopAndWait.wait();
					}
					catch(InterruptedException e){
						e.printStackTrace();
					}
				}
			}
			// do a checkpoint if at least CPT transactions have entered, and at least half of them have committed.
			if(enteredTxnsCount>=CHECKPOINT_TRIGGER && committedTrxns.get() >= (CHECKPOINT_TRIGGER/2)){
				//note here that the check pointing is being done on a thread which has not been allocated a Xid yet.
				stopIncoming(); 
				System.out.println("Checkpointing....: "+ myRMIName);
			}//else check if already some process is trying to stop incoming
			if(activeTxns.containsKey(xid)){
				// HOW TO HANDLE THIS ?
				System.out.println("SHOULD NOT REACH: XID DUPLICATE: "+ myRMIName);
			}
			enteredTxnsCount++;
		}

		synchronized(HashSetEmpty){
			activeTxns.put(xid,DUMMY);
			HashSetEmpty.set(false);
		}

		//<----------UNDOING--------------------->
		UndoIMTable.put(xid,new Stack<UndoIMLog>() );
		//</----------UNDOING--------------------->
		System.out.println("started succesfully");
		return ;
	}

	public void removeXID (int xid) throws InvalidTransactionException, TransactionAbortedException, RemoteException
	{
		isRegistered(xid);
		synchronized(activeTxns){
			System.out.println("About the remove entry from hashmap: "+ myRMIName);
			activeTxns.remove(xid);
			System.out.println("Done removing from hashmap: "+ myRMIName);
			if(activeTxns.size()==shuttingDown.get()){
				System.out.println("Active transactions are virtually empty: "+ myRMIName);
				HashSetEmpty.set(true);
			}
			System.out.println("Notifying");
			synchronized(HashSetEmpty){
				HashSetEmpty.notify();
			}
			System.out.println("Notified");
		}
		System.out.println("About the remove xid from UndoIMTable: "+ xid +" in: "+ myRMIName);
		UndoIMTable.remove(xid);
		System.out.println("Releasing all the locks for: "+xid+" in: "+ myRMIName);
		lockManager.unlockAll(xid);
		System.out.println("Returning from removeXID method: "+ myRMIName);
		return;
	}


	public boolean commit(int xid)
			throws RemoteException, 
			TransactionAbortedException, 
			InvalidTransactionException {
		System.out.println("Committing");
		// When xid is removed from the hashset , see if the hashset becomes equal to the shuttingDown.get() value -
		// implies there are no more useful processes left. hence can shutdown the system.

		//Check if transaction is already commited, If so, just return true
		if(comtdTxns.containsKey(xid))
			return true;

		synchronized(dieBeforePointerSwitch)
		{
			if(dieBeforePointerSwitch)
			{
				dieNow();
			}

		}

		Future returnVal = executor.submit(new TransactionLogger(xid+" " + "COMMIT\n", logWriter));
		try{
			returnVal.get();
		}
		catch(Exception e){
			System.out.println("Something hapened while retrieving value of atomic integer retunVal.Lets all zink about zees now"+e.getMessage());
		}
		logWriter.flush();
		synchronized(dieAfterPointerSwitch){
			if(dieAfterPointerSwitch){
				dieNow();
			}
		}
		removeXID(xid);
		comtdTxns.put(xid, DUMMY);
		System.out.println("Done commiting: "+xid+" in: "+ myRMIName);
		return true;
	}

	//<----------UNDOING--------------------->
	public void performUndo(UndoIMLog entry)
	{

		switch(entry.tableName)
		{
		case FlightTable:
			if(entry.operation==insert){
				System.out.println("insert in flight..undoing...");
				System.out.println(entry.Key);
				System.out.println("size of flighttable before undo"+flightTable.size());
				flightTable.remove(entry.Key);
				System.out.println("size of flighttable after undo"+flightTable.size());
			}
			else if(entry.operation == overWrite){
				Flight oldData = (Flight)(entry.ObjPointer);
				Flight newData = flightTable.get(entry.Key);
				newData.copyFlight(oldData);
			}
			else if(entry.operation == delete){
				flightTable.put(entry.Key,(Flight)(entry.ObjPointer));
			}
			break;
		case HotelTable:
			if(entry.operation==insert){
				hotelTable.remove(entry.Key);
			}
			else if(entry.operation == overWrite){
				Hotels oldData = (Hotels)(entry.ObjPointer);
				Hotels newData = hotelTable.get(entry.Key);
				newData.copyHotels(oldData);
			}
			//Not required(Will never happen)
			else if(entry.operation == delete){
				hotelTable.put(entry.Key,(Hotels)(entry.ObjPointer));
			}
			break;
		case CarTable:
			if(entry.operation==insert){
				carTable.remove(entry.Key);
			}
			else if(entry.operation == overWrite){
				Car oldData = (Car)(entry.ObjPointer);
				Car newData = carTable.get(entry.Key);
				newData.copyCar(oldData);
			}
			else if(entry.operation == delete){
				carTable.put(entry.Key,(Car)(entry.ObjPointer));
			}
			break;
		case ReservationTable:
			if(entry.operation==insert){
				reservationTable.remove(entry.Key);
			}
			else if(entry.operation == partialInsert){
				HashSet<Reservation> checkForFlights = reservationTable.get(entry.Key);
				checkForFlights.remove(entry.AuxKey);
			}
			else if(entry.operation == delete){
				HashSet<Reservation> ref = (HashSet<Reservation>)(entry.ObjPointer);
				reservationTable.put(entry.Key,ref);
			}
			break;
		case Flights:
			if(entry.operation==insert){
				reservedflights.remove(entry.Key);
			}
			else if(entry.operation == overWrite){
				Integer oldData = (Integer)(entry.ObjPointer);
				Integer newData = (Integer)reservedflights.get(entry.Key);
				newData = oldData;
			}
			else if(entry.operation == delete){
				//has to be decide based on kewals design.
			}
			break;
		default:
			System.out.println("Should not happen: " + entry.tableName + " in: " + myRMIName);
			break;
		}
	}
	//</----------UNDOING--------------------->

	//Undo all the work that has bee done by the transaction.
	public void abort(int xid)
			throws RemoteException, 
			InvalidTransactionException {
		//When xid is removed from the hashset , see if the hashset becomes empty, if so notify the hashSetEmpty thread: Done in removeXID.
		//<----------UNDOING--------------------->

		//TODO: Changes to be verified

		//Check if transaction is already aborted, if so, just return
		if(abrtdTxns.containsKey(xid))
			return;

		try {
			isRegistered(xid);
		} catch (TransactionAbortedException e1) {
			System.out.println("FadddieXID type case, Abort is called as first");
			return;
		}

		Stack<UndoIMLog> undo = UndoIMTable.get(xid);
		int retries=3;
		UndoIMLog entry = null;
		while(!undo.empty() && retries>0)
		{

			try
			{
				entry = undo.peek();
				if(entry==null){
					System.out.println("WHY IS THIS NULL?: "+ myRMIName);
				}
				performUndo(entry);
			}
			catch(Exception e)
			{
				retries--;
				System.out.println("Retyring to abort the last operation");
				System.out.println("retry number"+(3-retries));
				continue;
			}
			undo.pop();
		}
		//</----------UNDOING--------------------->
		Future returnVal = executor.submit(new TransactionLogger(xid+" " + "ABORT\n", logWriter));
		try
		{
			returnVal.get();
		}
		catch(Exception e)
		{
			System.out.println("Something hapened while retrieving value of atomic integer retunVal.Lets all zink about zees now"+e.getMessage());
		}
		logWriter.flush();
		
		System.out.println(" Aborted");

		try {
			removeXID(xid);
		} catch (TransactionAbortedException e) {
			System.out.println("Transaction aborted while removing in abort");
		}
		abrtdTxns.put(xid, DUMMY);
		return;
	}

	// ADMINISTRATIVE INTERFACE
	public boolean addFlight(int xid, String flightNum, int numSeats, int price) 
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {
		System.out.println("ResourceManager: entered addFlight");
		String lockString = "Flight."+flightNum;

		System.out.println("ResourceManager: about to check validity");
		//Check if valid XID
		isRegistered(xid);
		System.out.println("ResourceManager: done checking validity of transaction");

		

		try {
			if(lockManager.lock(xid, lockString, WRITE) == false){
				// Abort and then throw transaction aborted exception
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + flightNum);
			}
		} catch (DeadlockException e) {
			// Handle DeadLock !
			tm.abort(xid);
			e.printStackTrace();
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}

		int numAvail = numSeats;

		//<----------UNDOING--------------------->
		Flight OldVal = null;
		UndoIMLog logRec = null;
		//</----------UNDOING--------------------->

		StringBuilder logMsg = new StringBuilder("");
		if(flightTable.containsKey(flightNum)){
			Flight oldData = flightTable.get(flightNum);

			//<----------UNDOING--------------------->
			OldVal = new Flight(oldData);
			logRec = new UndoIMLog(FlightTable,overWrite,OldVal,flightNum,null);
			//</----------UNDOING--------------------->

			if(price>=0){
				oldData.setPrice(price);
				logMsg.append(xid).append("@#@").append("Flights@#@").append(flightNum).append("@#@").append("Price@#@").append(oldData.getPrice()).append("@#@").append(price).append("\n");
			}
			numAvail = numAvail + oldData.getNumAvail();
			oldData.setNumAvail(numAvail);
			logMsg.append(xid).append("@#@").append("Flights@#@").append(flightNum).append("@#@").append("NumAvail@#@").append(oldData.getNumAvail()).append("@#@").append(numAvail).append("\n");
			numSeats = numSeats + oldData.getNumSeats();
			oldData.setNumSeats(numSeats);
			logMsg.append(xid).append("@#@").append("Flights@#@").append(flightNum).append("@#@").append("NumSeats@#@").append(oldData.getNumSeats()).append("@#@").append(numSeats).append("\n");

		}
		else{
			Flight newData = new Flight(flightNum, price, numSeats, numAvail);

			//<----------UNDOING--------------------->
			logRec = new UndoIMLog(FlightTable,insert,null,flightNum,null);
			//</----------UNDOING--------------------->

			flightTable.put(flightNum, newData);
			logMsg.append(xid).append("@#@").append("Flights@#@").append(flightNum).append("@#@@#@INSERT\n");
			logMsg.append( xid).append("@#@").append("Flights@#@").append(flightNum).append("@#@").append("Price@#@").append("NULL").append("@#@").append(price).append("\n");
			logMsg.append( xid).append("@#@").append("Flights@#@").append(flightNum).append("@#@").append("NumAvail@#@").append("NULL").append("@#@").append(numAvail).append("\n");
			logMsg.append( xid).append("@#@").append("Flights@#@").append(flightNum).append("@#@").append("NumSeats@#@").append("NULL").append("@#@").append(numSeats).append("\n");
		}

		//<----------UNDOING--------------------->
		Stack<UndoIMLog> undo = UndoIMTable.get(xid);
		undo.push(logRec);
		//</----------UNDOING--------------------->

		executor.execute(new VariableLogger(logMsg.toString(), logWriter));
		return true;
	}

	public boolean deleteFlight(int xid, String flightNum)
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {
		if(reservedflights.containsKey(flightNum) && reservedflights.get(flightNum)!=0){
			return false;
		}

		String lockString = "Flight."+flightNum;
		StringBuilder logMsg = new StringBuilder("");

		//Check if valid XID
		isRegistered(xid);

		try {
			if(lockManager.lock(xid, lockString, WRITE) == false){
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + flightNum);
			}
		} catch (DeadlockException e) {
			tm.abort(xid);
			e.printStackTrace();
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		if(!flightTable.containsKey(flightNum)){
			return false;
		}

		//<----------UNDOING--------------------->
		Flight OldVal = flightTable.get(flightNum);
		UndoIMLog logRec = new UndoIMLog(FlightTable,delete,OldVal,flightNum,null);;
		//</----------UNDOING--------------------->

		flightTable.remove(flightNum);

		//<----------UNDOING--------------------->
		Stack<UndoIMLog> undo = UndoIMTable.get(xid);
		undo.push(logRec);
		//</----------UNDOING--------------------->

		logMsg.append(xid).append("@#@").append("Flights@#@").append(flightNum).append("@#@@#@DELETE\n");
		executor.execute(new VariableLogger(logMsg.toString(), logWriter));
		return true;
	}

	/*
	 * Add rooms to a location.  
	 * This should look a lot like addFlight, only keyed on a location
	 * instead of a flight number.
	 *
	 * @return true on success, false on failure.
	 *
	 * @throws RemoteException on communications failure.
	 * @throws TransactionAbortedException if transaction was aborted.
	 * @throws InvalidTransactionException if transaction id is invalid.
	 *
	 * @see #addFlight
	 */
	public boolean addRooms(int xid, String location, int numRooms, int price) 
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {

		isRegistered(xid);
		try{
			if(location==null)
				return false;
			String lockString = "Hotels."+location;
			if(lockManager.lock(xid, lockString, WRITE) == false){
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}
			//<----------UNDOING--------------------->
			Hotels OldVal = null;
			UndoIMLog logRec = null;
			//</----------UNDOING--------------------->

			int numAvail = numRooms;
			StringBuilder logMsg = new StringBuilder("");
			if(hotelTable.containsKey(location)){

				Hotels oldData = hotelTable.get(location);

				//<----------UNDOING--------------------->
				OldVal = new Hotels(oldData);
				logRec = new UndoIMLog(HotelTable,overWrite,OldVal,location,null);
				//</----------UNDOING--------------------->

				if(price>=0){
					oldData.setPrice(price);
					logMsg.append(xid).append("@#@").append("Rooms@#@").append(location).append("@#@").append("Price@#@").append(oldData.getPrice()).append("@#@").append(price).append("\n");
				}
				numAvail = numAvail + oldData.getNumAvail();
				oldData.setNumAvail(numAvail);
				logMsg.append( xid).append("@#@").append("Rooms@#@").append(location).append("@#@").append("NumAvail@#@").append(oldData.getNumAvail()).append("@#@").append(numAvail).append("\n");
				numRooms = numRooms + oldData.getnumRooms();
				oldData.setNumRooms(numRooms);
				logMsg.append(xid).append("@#@").append("Rooms@#@").append(location).append("@#@").append("NumRooms@#@").append(oldData.getnumRooms()).append("@#@").append(numRooms).append("\n");
			}
			else{

				//<----------UNDOING--------------------->
				Hotels newData = new Hotels(location, price, numRooms, numAvail);
				logRec = new UndoIMLog(HotelTable,insert,null,location,null);
				//</----------UNDOING--------------------->

				hotelTable.put(location, newData);
				logMsg.append(xid).append("@#@").append("Rooms@#@").append(location).append("@#@@#@INSERT\n");
				logMsg.append(xid).append("@#@").append("Rooms@#@").append(location).append("@#@").append("Price@#@").append("NULL").append("@#@").append(price).append("\n");
				logMsg.append(xid).append("@#@").append("Rooms@#@").append(location).append("@#@").append("NumAvail@#@").append("NULL").append("@#@").append(numAvail).append("\n");
				logMsg.append(xid).append("@#@").append("Rooms@#@").append(location).append("@#@").append("NumRooms@#@").append("NULL").append("@#@").append(numRooms).append("\n");
			}

			//<----------UNDOING--------------------->
			Stack<UndoIMLog> undo = UndoIMTable.get(xid);
			undo.push(logRec);
			//</----------UNDOING--------------------->

			executor.execute(new VariableLogger(logMsg.toString(), logWriter));
			return true;	
		}catch (DeadlockException e) {
			tm.abort(xid);
			e.printStackTrace();
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		catch( Exception e)	{
			tm.abort(xid);
			e.printStackTrace();
			throw new TransactionAbortedException(xid, "Aborted transaction because 'Other' Exception found: "+xid);

		}
	}

	/**
	 * Delete rooms from a location.
	 * This subtracts from the available room count (rooms not allocated
	 * to a customer).  It should fail if it would make the count of
	 * available rooms negative.
	 *
	 * @return true on success, false on failure.
	 *
	 * @throws RemoteException on communications failure.
	 * @throws TransactionAbortedException if transaction was aborted.
	 * @throws InvalidTransactionException if transaction id is invalid.
	 *
	 * @see #deleteFlight
	 */
	public boolean deleteRooms(int xid, String location, int numRooms) 
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {

		//throw InvalidTransactionException;
		isRegistered(xid);
		try{
			if(location==null)
				return false;

			String lockString = "Hotels."+location;
			if(lockManager.lock(xid, lockString, WRITE) == false){
				return false;
			}
			StringBuilder logMsg = new StringBuilder("");
			int numAvail = 0;
			if(hotelTable.containsKey(location)){

				Hotels data = hotelTable.get(location);
				numAvail = data.getNumAvail();
				if(numRooms>numAvail)
					return false;

				//<----------UNDOING--------------------->
				Hotels OldVal = new Hotels(data);
				UndoIMLog logRec = new UndoIMLog(HotelTable,overWrite,OldVal,location,null);;
				Stack<UndoIMLog> undo = UndoIMTable.get(xid);
				undo.push(logRec);
				//</----------UNDOING--------------------->

				data.setNumAvail(numAvail-numRooms);
				logMsg.append(xid).append("@#@").append("Rooms@#@").append(location).append("@#@").append("NumAvail@#@").append(numAvail).append("@#@").append(numAvail - numRooms).append("\n");
				data.setNumRooms(data.getnumRooms()-numRooms);
				logMsg.append(xid).append("@#@").append("Rooms@#@").append(location).append("@#@").append("NumRooms@#@").append(data.getnumRooms()).append("@#@").append(data.getnumRooms() - numRooms).append("\n");
			}
			else{
				// should not happen ... if it happens return false.
				return false;
			}
			executor.execute(new VariableLogger(logMsg.toString(), logWriter));
			return true;

		}catch (DeadlockException e) {
			tm.abort(xid);
			e.printStackTrace();
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		catch( Exception e)	{
			tm.abort(xid);
			e.printStackTrace();
			throw new TransactionAbortedException(xid, "Aborted transaction because 'Other' Exception found: "+xid);

		}
	}


	public boolean addCars(int xid, String location, int numCars, int price) 
			throws RemoteException, 

			TransactionAbortedException,
			InvalidTransactionException {

		isRegistered(xid);		
		String lockString = "Cars."+location;
		StringBuilder logMsg = new StringBuilder("");
		try {
			if(lockManager.lock(xid, lockString, WRITE) == false){
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}

			int numAvail = numCars;

			//<----------UNDOING--------------------->
			Car OldVal = null;
			UndoIMLog logRec = null;
			//</----------UNDOING--------------------->

			if(carTable.containsKey(location)){
				Car oldData = carTable.get(location);

				//<----------UNDOING--------------------->
				OldVal = new Car(oldData);
				logRec = new UndoIMLog(CarTable,overWrite,OldVal,location,null);
				//</----------UNDOING--------------------->

				if(price>=0){
					oldData.setPrice(price);
					logMsg.append(xid).append("@#@").append("Cars@#@").append(location).append("@#@").append("Price@#@").append(oldData.getPrice()).append("@#@").append(price).append("\n");
				}
				numAvail = numAvail + oldData.getNumAvail();
				oldData.setNumAvail(numAvail);
				logMsg.append( xid).append("@#@").append("Cars@#@").append(location).append("@#@").append("NumAvail@#@").append(oldData.getNumAvail()).append("@#@").append(numAvail).append("\n");
				numCars = numCars + oldData.getNumCars();
				oldData.setNumCars(numCars);
				logMsg.append(xid).append("@#@").append("Cars@#@").append(location).append("@#@").append("NumCars@#@").append(oldData.getNumCars()).append("@#@").append(numCars).append("\n");
			}
			else{
				Car newData = new Car(location, price, numCars, numAvail);
				carTable.put(location, newData);

				//<----------UNDOING--------------------->
				logRec = new UndoIMLog(CarTable,insert,null,location,null);
				//</----------UNDOING--------------------->
				logMsg.append(xid).append("@#@").append("Cars@#@").append(location).append("@#@@#@INSERT\n");
				logMsg.append(xid).append("@#@").append("Cars@#@").append(location).append("@#@").append("Price@#@").append("NULL").append("@#@").append(price).append("\n");
				logMsg.append(xid).append("@#@").append("Cars@#@").append(location).append("@#@").append("NumAvail@#@").append("NULL").append("@#@").append(numAvail).append("\n");
				logMsg.append(xid).append("@#@").append("Cars@#@").append(location).append("@#@").append("NumCars@#@").append("NULL").append("@#@").append(numCars).append("\n");
			}
			////<----------UNDOING--------------------->
			Stack<UndoIMLog> undo = UndoIMTable.get(xid);
			undo.push(logRec);
			//</----------UNDOING--------------------->

		} catch (DeadlockException e) {
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);

		} catch (Exception e) {
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Transaction aborted for unknown reasons" + "MSG: " + e.getMessage());
		}
		executor.execute(new VariableLogger(logMsg.toString(), logWriter));
		return true;
	}

	public boolean deleteCars(int xid, String location, int numCars) 
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {
		isRegistered(xid);
		String lockString = "Cars."+location;
		StringBuilder logMsg = new StringBuilder("");
		try {
			if(lockManager.lock(xid, lockString, WRITE) == false){
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}

			if(carTable.containsKey(location)){
				Car oldData = carTable.get(location);
				int numCarsAvail = oldData.getNumAvail();
				if(numCarsAvail >= numCars){
					//Delete successfully
					logMsg.append(xid).append("@#@").append("Cars@#@").append(location).append("@#@").append("NumAvail@#@").append(numCarsAvail).append("@#@").append(numCarsAvail - numCars).append("\n");

					//<----------UNDOING--------------------->
					Car OldVal = new Car(oldData);
					UndoIMLog logRec = new UndoIMLog(CarTable,overWrite,OldVal,location,null);;
					Stack<UndoIMLog> undo = UndoIMTable.get(xid);
					undo.push(logRec);
					//</----------UNDOING--------------------->

					oldData.setNumAvail(numCarsAvail - numCars);
					logMsg.append(xid).append("@#@").append("Cars@#@").append(location).append("@#@").append("NumCars@#@").append(oldData.getNumCars()).append("@#@").append(oldData.getNumCars() - numCars).append("\n");
					oldData.setNumCars(oldData.getNumCars() - numCars);
				}else{
					return false;
				}
			}else{
				return false;
			}
		} catch (DeadlockException e) {
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		} catch (Exception e) {
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because 'Other' Exception found: "+xid);
		}
		executor.execute(new VariableLogger(logMsg.toString(), logWriter));
		return true;
	}

	// Make a new entry in Reservations Table for this Customer.
	// If customer already exists ?
	public boolean newCustomer(int xid, String custName) 
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {
		isRegistered(xid);

		// Null Customer Name
		if(custName==null)
			return false;
		try{
			// Acquire Lock
			String lockString = "Reservations." + custName;
			if(lockManager.lock(xid, lockString, WRITE) == false){
				return false;
			}

			//Check if customer already exists
			//ASK KEWAL TO CHANGE THIS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
			if(!reservationTable.containsKey(custName)){
				reservationTable.put(custName, new HashSet<Reservation>());

				//<----------UNDOING--------------------->
				UndoIMLog logRec = new UndoIMLog(ReservationTable,insert,null,custName,null);
				Stack<UndoIMLog> undo = UndoIMTable.get(xid);
				undo.push(logRec);
				//</----------UNDOING--------------------->

			}

		}catch (DeadlockException e) {
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		catch( Exception e)	{
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because 'Other' Exception found: "+xid);
		}
		return true;
	}

	public boolean deleteCustomer(int xid, String custName) 
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {
		// Null Customer Name
		if(custName==null)
			return false;
		try{
			// Acquire Lock
			String lockString = "Reservations." + custName;
			if(lockManager.lock(xid, lockString, WRITE) == false){
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}

			//Check if customer exists
			if(!reservationTable.containsKey(custName)){
				return false;
			}

			//Over Here Customer exists
			//Check if customer has made any flight reservations
			HashSet<Reservation> checkForFlights = reservationTable.get(custName);
			UndoIMLog logRec = null;
			Stack<UndoIMLog> undo = null;
			undo = UndoIMTable.get(xid);

			for (Reservation r : checkForFlights) {
				String key = r.getResKey(); 
				int numAvail = 0;
				switch(r.getResType()){
				case 1:
					lockString = "Flight." + key;
					if(lockManager.lock(xid, lockString, WRITE) == false){
						tm.abort(xid);
						throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
					}
					// Reduce number of seats reserved in reserved flights
					int avail = (Integer)reservedflights.get(key);

					//<----------UNDOING--------------------->
					Integer oldVal= new Integer(avail);
					logRec = new UndoIMLog(Flights,overWrite,oldVal,key,null);
					undo.push(logRec);
					//</----------UNDOING--------------------->

					reservedflights.put(r.getResKey(),avail-1);

					// Increase number of seats available in that particular flight
					Flight flight = flightTable.get(key);

					//<----------UNDOING--------------------->
					Flight oldValF= new Flight(flight);
					logRec = new UndoIMLog(FlightTable,overWrite,oldValF,key,null);
					undo.push(logRec);
					//</----------UNDOING--------------------->

					numAvail = flight.getNumAvail();
					flight.setNumAvail(numAvail+1);

					break;
				case 2:
					lockString = "Hotels." + key;
					if(lockManager.lock(xid, lockString, WRITE) == false){
						tm.abort(xid);
						throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
					}
					// Increase number of rooms available in that particular Hotel Location
					Hotels hotel = hotelTable.get(key);

					//<----------UNDOING--------------------->
					Hotels oldValH= new Hotels(hotel);
					logRec = new UndoIMLog(HotelTable,overWrite,oldValH,key,null);
					undo.push(logRec);
					//</----------UNDOING--------------------->

					numAvail = hotel.getNumAvail();
					hotel.setNumAvail(numAvail+1);
					break;
				case 3:
					lockString = "Cars." + key;
					if(lockManager.lock(xid, lockString, WRITE) == false){
						tm.abort(xid);
						throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
					}
					// Increase number of cars available in that particular Car location
					Car car = carTable.get(key);

					//<----------UNDOING--------------------->
					Car oldValC= new Car(car);
					logRec = new UndoIMLog(CarTable,overWrite,oldValC,key,null);
					undo.push(logRec);
					//</----------UNDOING--------------------->

					numAvail = car.getNumAvail();
					car.setNumAvail(numAvail+1);
					break;
				default:
					break;
				}
			}

			reservationTable.remove(custName);

			//<----------UNDOING--------------------->
			logRec = new UndoIMLog(ReservationTable,delete,checkForFlights,custName,null);
			undo = UndoIMTable.get(xid);
			undo.push(logRec);
			//</----------UNDOING--------------------->

		}catch (DeadlockException e) {
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		catch( Exception e)	{
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because 'Other' Exception found: "+xid);
		}
		return true;
	}


	// QUERY INTERFACE
	public int queryFlight(int xid, String flightNum)
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {
		//Check for invalid xid.
		isRegistered(xid);

		if(flightNum == null)
			throw new InvalidTransactionException(xid, "message");

		// Acquiring read locks
		String lockString = "Flight."+flightNum;
		try {
			if(lockManager.lock(xid, lockString, READ) == false){
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}
		} catch (DeadlockException e) {
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		if(!flightTable.containsKey(flightNum)){
			return 0;
		}
		Flight flight = flightTable.get(flightNum);
		return flight.getNumAvail();
	}

	public int queryFlightPrice(int xid, String flightNum)
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {
		//Check for invalid xid.
		isRegistered(xid);

		if(flightNum == null)
			throw new InvalidTransactionException(xid, "message");

		// Acquiring read locks
		String lockString = "Flight."+flightNum;
		try {
			if(lockManager.lock(xid, lockString, READ) == false){
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}
		} catch (DeadlockException e) {
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		if(!flightTable.containsKey(flightNum)){
			return 0;
		}
		Flight flight = flightTable.get(flightNum);
		return flight.getPrice();
	}

	/**
	 * Return the number of rooms available at a location. 
	 * Implies whole deletion of the location record: all cars, all reservations.
	 * Should fail if a customer has booked a car from this location.
	 *
	 * @param xid id of transaction.
	 * @param location , cannot be null.
	 * @return 0 on 0 availability or absence of record, else return available 
	 *
	 * @throws RemoteException on communications failure.
	 * @throws TransactionAbortedException if transaction was aborted.
	 * @throws InvalidTransactionException if transaction id is invalid.
	 *
	 * @see #deleteRooms
	 * @see #deleteFlight
	 */
	public int queryRooms(int xid, String location)
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {

		//throw InvalidTransactionException;
		isRegistered(xid);

		if(location==null)
			return 0;
		try{
			String lockString = "Hotels."+location;
			if(lockManager.lock(xid, lockString, READ) == false){
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}
			int numAvail =0;
			if(hotelTable.containsKey(location)){

				Hotels oldData = hotelTable.get(location);
				numAvail = oldData.getNumAvail();
			}
			return numAvail;	
		}catch (DeadlockException e) {
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		catch( Exception e)	{
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because 'Other' Exception found: "+xid);
		}

	}
	/**
	 * Return the price of rooms at this location.
	 * Implies whole deletion of the location record: all cars, all reservations.
	 * Should fail if a customer has booked a car from this location.
	 *
	 * @param xid id of transaction.
	 * @param location , cannot be null.
	 * @return 0 on 0 availability or absence of record, else return actual price.
	 *
	 * @throws RemoteException on communications failure.
	 * @throws TransactionAbortedException if transaction was aborted.
	 * @throws InvalidTransactionException if transaction id is invalid.
	 *
	 * @see #deleteRooms
	 * @see #deleteFlight
	 */

	public int queryRoomsPrice(int xid, String location)
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {

		//throw InvalidTransactionException;
		isRegistered(xid);
		if(location==null)
			return 0;
		try{
			String lockString = "Hotels."+location;
			if(lockManager.lock(xid, lockString, READ) == false){
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}
			int price =0;
			if(hotelTable.containsKey(location)){

				Hotels oldData = hotelTable.get(location);
				price = oldData.getPrice();
			}
			return price;	
		}catch (DeadlockException e) {
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		catch( Exception e)	{
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because 'Other' Exception found: "+xid);
		}
	}

	public int queryCars(int xid, String location)
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {
		if(location==null)
		{
			return 0;
		}
		isRegistered(xid);
		String lockString = "Cars."+location;
		try {
			if(lockManager.lock(xid, lockString, READ) == false){
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}

			if(carTable.containsKey(location)){
				return carTable.get(location).getNumAvail();
			}else{
				return 0;
			}
		} catch (DeadlockException e) {
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		} catch (Exception e) {
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because 'Other' Exception found: "+xid);
		}
	}

	public int queryCarsPrice(int xid, String location)
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {
		isRegistered(xid);
		String lockString = "Cars."+location;
		try {
			if(lockManager.lock(xid, lockString, READ) == false){
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}

			if(carTable.containsKey(location)){
				return carTable.get(location).getPrice();
			}else{
				return 0;
			}
		} catch (DeadlockException e) {
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		} catch (Exception e) {
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because 'Other' Exception found: "+xid);
		}
	}

	public int queryCustomerBill(int xid, String custName)
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {

		if(custName==null){
			System.out.println("Customer name is: "+custName);
			return 0;
		}
		int customerBill = 0;
		try{
			// Acquire Lock
			String lockString = "Reservations." + custName;
			if(lockManager.lock(xid, lockString, READ) == false){
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}

			//Check if customer exists
			if(!reservationTable.containsKey(custName)){
				
				return 0;
			}
			//Over Here Customer exists
			//Check if customer has made any flight reservations
			HashSet<Reservation> checkForFlights = reservationTable.get(custName);
			for (Reservation r : checkForFlights) {
				String key = r.getResKey();
				switch(r.getResType()){
				case 1:
					lockString = "Flight." + key;
					if(lockManager.lock(xid, lockString, READ) == false){
						tm.abort(xid);
						throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
					}
					customerBill += flightTable.get(key).getPrice();
					break;
				case 2:
					lockString = "Hotels." + key;
					if(lockManager.lock(xid, lockString, READ) == false){
						tm.abort(xid);
						throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
					}
					customerBill += hotelTable.get(key).getPrice();
					break;
				case 3:
					lockString = "Cars." + key;
					if(lockManager.lock(xid, lockString, READ) == false){
						tm.abort(xid);
						throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
					}
					customerBill += carTable.get(key).getPrice();
					break;
				default:
					break;
				}
			}

		}catch (DeadlockException e) {
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		catch( Exception e)	{
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because 'Other' Exception found: "+xid);
		}
		return customerBill;
	}


	// RESERVATION INTERFACE
	public boolean reserveFlight(int xid, String custName, String flightNum) 
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {
		// Check for valid xid
		isRegistered(xid);

		String lockString = "Flight." + flightNum;
		try {
			if(lockManager.lock(xid, lockString, WRITE) == false){
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString); 	
			}
			lockString = "Reservations."+custName;
			if(lockManager.lock(xid, lockString, WRITE) == false){
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);

			}
		} catch (DeadlockException e) {
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		if(!flightTable.containsKey(flightNum)){
			return false;
		}
		Flight data = flightTable.get(flightNum);
		int avail = data.getNumAvail();
		if(avail < 1){
			return false;
		}

		//<----------UNDOING--------------------->//entry for flights table
		Flight oldVal = new Flight(data);
		UndoIMLog logRec=new UndoIMLog(FlightTable,overWrite,oldVal,flightNum,null);
		Stack<UndoIMLog> undo = UndoIMTable.get(xid);
		undo.push(logRec);
		logRec=null;
		//</----------UNDOING--------------------->

		Reservation newReservation = new Reservation(custName, 1, flightNum);
		HashSet<Reservation> reservations;
		StringBuilder logMsg = new StringBuilder("");
		if(reservationTable.containsKey(custName)){
			//Customer has a reservation
			reservations = reservationTable.get(custName);
			if(reservations.contains(newReservation)){
				return true;
			}
		}else{
			// First reservation for this customer.
			// Create a new hashset

			//<----------UNDOING--------------------->//entry for reservations table.
			logRec = new UndoIMLog(ReservationTable,insert,null,custName,null);
			//<----------UNDOING--------------------->

			reservations = new HashSet<Reservation>();
			reservationTable.put(custName, reservations);
		}

		//<----------UNDOING--------------------->
		if(logRec==null)
			logRec = new UndoIMLog(ReservationTable,partialInsert,null,custName,newReservation);
		undo.push(logRec);
		//<----------UNDOING--------------------->

		//Sure of making a reservation
		reservations.add(newReservation);
		logMsg.append(xid).append("@#@").append("Reservations@#@").append(newReservation.toString()).append("@#@@#@INSERT\n");

		//Make entry in flights because reservation is made
		if(!reservedflights.containsKey(flightNum))
		{
			reservedflights.put(flightNum,1);

			//<----------UNDOING--------------------->
			logRec = new UndoIMLog(Flights,insert,null,flightNum,null);
			//<----------UNDOING--------------------->
		}
		else
		{

			//<----------UNDOING--------------------->
			Integer num=(Integer)reservedflights.get(flightNum);
			logRec = new UndoIMLog(Flights,overWrite,num,flightNum,null);
			//<----------UNDOING--------------------->

			reservedflights.put(flightNum, num+1);
		}

		//Decrement number of available seats
		System.out.println("number of seats before"+data.getNumAvail());
		data.setNumAvail(avail - 1);
		System.out.println("number of seats after booking"+data.getNumAvail());
		//</----------UNDOING--------------------->
		undo.push(logRec);
		//</----------UNDOING--------------------->

		logMsg.append(xid).append("@#@").append("Flights@#@").append(flightNum).append("@#@").append("NumAvail@#@").append(avail).append("@#@").append(avail - 1).append("\n");
		executor.execute(new VariableLogger(logMsg.toString(), logWriter));
		return true;
	}

	public boolean reserveCar(int xid, String custName, String location) 
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {

		isRegistered(xid);
		String lockString = "Cars." + location;
		try {
			if(lockManager.lock(xid, lockString, WRITE) == false){
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}
			lockString = "Reservations."+custName;
			if(lockManager.lock(xid, lockString, WRITE) == false){
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}
		} catch (DeadlockException e) {
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		int numCarsAvail = 0;
		Car data = carTable.get(location);
		if(data != null){
			numCarsAvail = data.getNumAvail();
		}
		if(!(numCarsAvail > 0)){
			return false;
		}

		//<----------UNDOING--------------------->//entry for flights table
		Car oldVal = new Car(data);
		UndoIMLog logRec=new UndoIMLog(CarTable,overWrite,oldVal,location,null);
		Stack<UndoIMLog> undo = UndoIMTable.get(xid);
		undo.push(logRec);
		logRec = null;
		//</----------UNDOING--------------------->	

		Reservation newReservation = new Reservation(custName, 3, location);
		HashSet<Reservation> reservations;
		StringBuilder logMsg = new StringBuilder("");
		if(reservationTable.containsKey(custName)){
			//Customer has a reservation
			reservations = reservationTable.get(custName);
			// DUplicate Reservation
			if(reservations.contains(newReservation)){
				return true;
			}

		}else{
			//<----------UNDOING--------------------->//entry for reservations table.
			logRec = new UndoIMLog(ReservationTable,insert,null,custName,null);
			//<----------UNDOING--------------------->

			reservations = new HashSet<Reservation>();
			reservationTable.put(custName, reservations);
		}

		//<----------UNDOING--------------------->
		if(logRec==null)
			logRec = new UndoIMLog(ReservationTable,partialInsert,null,custName,newReservation);
		undo.push(logRec);
		//<----------UNDOING--------------------->

		//Sure of making a reservation
		reservations.add(newReservation);
		logMsg.append(xid).append("@#@").append("Reservations@#@").append(newReservation.toString()).append("@#@@#@INSERT\n");
		data.setNumAvail(numCarsAvail - 1);
		logMsg.append(xid).append("@#@").append("Cars@#@").append(location).append("@#@").append("NumAvail@#@").append(numCarsAvail).append("@#@").append(numCarsAvail - 1).append("\n");
		executor.execute(new VariableLogger(logMsg.toString(), logWriter));
		return true;
	}

	public boolean reserveRoom(int xid, String custName, String location) 
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {
		isRegistered(xid);	
		try{
			String lockString = "Hotels." + location;

			if(lockManager.lock(xid, lockString, WRITE) == false){
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}
			lockString = "Reservations."+custName;
			if(lockManager.lock(xid, lockString, WRITE) == false){
				tm.abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}

			int numRoomsAvail = 0;
			Hotels data = hotelTable.get(location);
			if(data != null){
				numRoomsAvail = data.getNumAvail();
			}
			if(!(numRoomsAvail > 0)){
				return false;
			}
			//<----------UNDOING--------------------->//entry for flights table
			Hotels oldVal = new Hotels(data);
			UndoIMLog logRec=new UndoIMLog(HotelTable,overWrite,oldVal,location,null);
			Stack<UndoIMLog> undo = UndoIMTable.get(xid);
			undo.push(logRec);
			logRec = null;
			//</----------UNDOING--------------------->	

			Reservation newReservation = new Reservation(custName, 2, location);
			HashSet<Reservation> reservations;
			StringBuilder logMsg = new StringBuilder("");
			if(reservationTable.containsKey(custName)){
				//Customer has a reservation
				reservations = reservationTable.get(custName);
				// Duplicate Reservation
				if(reservations.contains(newReservation)){
					return true;
				}

			}else{
				//<----------UNDOING--------------------->//entry for reservations table.
				logRec = new UndoIMLog(ReservationTable,insert,null,custName,null);
				//<----------UNDOING--------------------->

				reservations = new HashSet<Reservation>();
				reservationTable.put(custName, reservations);
			}

			//<----------UNDOING--------------------->
			if(logRec==null)
				logRec = new UndoIMLog(ReservationTable,partialInsert,null,custName,newReservation);
			undo.push(logRec);
			//<----------UNDOING--------------------->

			//Sure of making a reservation
			reservations.add(newReservation);
			logMsg.append(xid).append("@#@").append("Reservations@#@").append(newReservation.toString()).append("@#@@#@INSERT\n");
			data.setNumAvail(numRoomsAvail - 1);
			logMsg.append(xid).append("@#@").append("Rooms@#@").append(location).append("@#@").append("NumAvail@#@").append(numRoomsAvail).append("@#@").append(numRoomsAvail - 1).append("\n");
			executor.execute(new VariableLogger(logMsg.toString(), logWriter));
			return true;
		}
		catch (DeadlockException e) {
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		catch( Exception e)	{
			e.printStackTrace();
			tm.abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because 'Other' Exception found: "+xid);
		}
	}

	// TECHNICAL/TESTING INTERFACE
	public boolean shutdown()
			throws RemoteException {

		shuttingDown.incrementAndGet();
		stopIncoming();
		try
		{
			Thread.sleep(SLEEPSHUTDOWN);
		}
		catch(InterruptedException e)
		{
			System.out.println("not enough sleep time.. increase it");

		}
		//testcases have to be analysed as to whether this thread has to freeze or can it return?
		return true;
	}

	public boolean dieNow() 
			throws RemoteException {
		logWriter.flush();
		System.exit(1);
		return true; // We won't ever get here since we exited above;
		// but we still need it to please the compiler.
	}

	public boolean dieBeforePointerSwitch() 
			throws RemoteException {
		synchronized(dieBeforePointerSwitch){
			//TODO: Should DieBeforeCommit be atomic boolean ?
			dieBeforePointerSwitch=dieBeforePointerSwitch.valueOf(true);
		}
		return true;
	}

	public boolean dieAfterPointerSwitch() 
			throws RemoteException {
		synchronized(dieAfterPointerSwitch){
			//TODO: Should DieAfterCommit be atomic boolean ?
			dieAfterPointerSwitch=dieAfterPointerSwitch.valueOf(true);
		}
		return true;
	}

	//RECOVERY/ STARTUP INTERFACE

	public void loadFiles() throws RemoteException,FileNotFoundException{
		LoadFiles loadObject = new LoadFiles(checkPointers);
		loadObject.loadSetup();
		if(loadObject.load(0)==false){
			// Shut the system down
			dieNow();
		}

		flightTable = (ConcurrentHashMap<String, Flight>) loadObject.getTR("flightTable").getTable();
		carTable = (ConcurrentHashMap<String, Car>) loadObject.getTR("carTable").getTable();
		hotelTable = (ConcurrentHashMap<String, Hotels>) loadObject.getTR("hotelTable").getTable();;
		reservationTable = (ConcurrentHashMap<String, HashSet<Reservation>>) loadObject.getTR("reservationTable").getTable();;
		reservedflights = (ConcurrentHashMap<String,Integer>) loadObject.getTR("reservedFlights").getTable();;
	}

	public void recover() throws FileNotFoundException{
		RecoveryManager recoveryManager = new RecoveryManager(flightTable,  carTable,  hotelTable, reservationTable,  reservedflights, logFile);
		System.out.println("Recovery Manager instantiated");
		boolean recoveryRequired = recoveryManager.analyze();
		abrtdTxns = recoveryManager.getAbrtdTxns();
		comtdTxns = recoveryManager.getComtdTxns();
		prprdTxns = recoveryManager.getPrprdTxns();
		xidCounter = recoveryManager.getMAXid() + 1;

		if(recoveryRequired==false){
			System.out.println("No Need to recover");
			return;
		}		
		System.out.println("Analyze phase done");
		if(recoveryManager.redo()==false){
			System.out.println("Failed during redo");
			return;
		}
		System.out.println("REDO phase done");
	}

	public boolean prepare(int xid) throws RemoteException {
		if(abrtdTxns.containsKey(xid)){
			return false;			
		}
		if(!activeTxns.containsKey(xid)){
			System.out.println("Should not reach here: in prepare");
		}
		return true;
	}

	public String getRMName() throws RemoteException {
		return myRMIName;
	}
}
// TM INTERFACE
/*	public void register(int xid){
		tm.enlist(xid,this);
	}*/



