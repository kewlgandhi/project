package transaction;

import lockmgr.*;
import transaction.recovery.LoadFiles;
import transaction.recovery.RecoveryManager;
import transaction.logmgr.LogWriter;
import transaction.logmgr.TransactionLogger;
import transaction.logmgr.VariableLogger;
import transaction.bean.*;

import java.io.FileNotFoundException;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.file.Files;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
/** 
 * Resource Manager for the Distributed Travel Reservation System.
 * 
 * Description: toy implementation of the RM, for initial testing
 */

public class ResourceManagerImplPhase1 
extends java.rmi.server.UnicastRemoteObject
 {

	//Book keeping and other variables
	private ConcurrentHashMap<Integer,Object> activeTxns;
	private LockManager lockManager;
	private volatile AtomicInteger shuttingDown = new AtomicInteger();
	private volatile AtomicInteger committedTrxns = new  AtomicInteger();
	private volatile Integer enteredTxnsCount=0;
	private AtomicBoolean stopAndWait = new AtomicBoolean(false);
	private AtomicBoolean HashSetEmpty = new AtomicBoolean(true);
	private Boolean DieBeforeCommit = new Boolean(false);
	private Boolean DieAfterCommit = new Boolean(false);
	private ExecutorService checkPointers ;
	private Set<Callable<Integer>> callables;
	private ExecutorService executor ;
	private ConcurrentHashMap<Integer, Object> abrtdTxns;
	private LogWriter logWriter;
	private String logFile;
 
	// Other Variables
	private static final Object DUMMY = new Object();
	private final int WRITE = 1;
	private final int READ = 0;
	private static final int CHECKPOINT_TRIGGER = 1;
	private static final int SLEEPSHUTDOWN = 5000;

	// Data Sets
	private ConcurrentHashMap<String,Flight> flightTable;
	private ConcurrentHashMap<String,Car> carTable;
	private ConcurrentHashMap<String,Hotels> hotelTable;
	private ConcurrentHashMap<String,HashSet<Reservation>> reservationTable;
	private ConcurrentHashMap<String,Integer> reservedflights;

	//<----------UNDOING--------------------->
	private ConcurrentHashMap<Integer,Stack<UndoIMLog> > UndoIMTable;
	//</----------UNDOING--------------------->

	protected int xidCounter;

	//<----------UNDOING--------------------->
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
//			rmiName = ResourceManager.DefaultRMIName;
			rmiName = "RM";
		}

		String rmiRegPort = System.getProperty("rmiRegPort");
		if (rmiRegPort != null && !rmiRegPort.equals("")) {
			rmiName = "//:" + rmiRegPort + "/" + rmiName;
		}

		try {
			ResourceManagerImplPhase1 obj = new ResourceManagerImplPhase1();
			Naming.rebind(rmiName, obj);
			System.out.println("RM bound");
		} 
		catch (Exception e) {
			System.err.println("RM not bound:" + e);
			System.exit(1);
		}
	}

	///TODO:
	//e) add a logging routine, which has to be invoke from all other routines. 
	//4. synchronous checkpointing needs to be done  in two cases
	//a) after n new transactions have entered the system.
	//b) when graceful system shutdown - in both cases we need to wait for the currently exeucting
	//   trnsactions to commit and then do it. 
	//c) Identify the places to abort , and places to return false for all database query routines.
	//d) implmenen the 4 die/shutdown routines.
	//e) check if excception handling is done in all the routines. 
	//f) design logging.
	//g) design the recoevey mechanism - impkemnt the abort/commit functions.
	//h ) restart/startup functions - to read from the disk ,waht to read, perform recovery on startup.
	//i) Add volatile to some variables
	void checkAndCreateData()
	{
		Path path = Paths.get("data");
		if(Files.notExists(path	))
		{
			File dir= new File("data");
			dir.mkdir();
		}
	}

	public ResourceManagerImplPhase1() throws RemoteException {
		System.out.println("starting constructor");
		lockManager = new LockManager();
		activeTxns = new ConcurrentHashMap<Integer,Object>();
		flightTable = new ConcurrentHashMap<String, Flight>();
		carTable = new ConcurrentHashMap<String, Car>();
		hotelTable = new ConcurrentHashMap<String, Hotels>();
		reservationTable = new ConcurrentHashMap<String, HashSet<Reservation>>();
		reservedflights = new ConcurrentHashMap<String,Integer>();
		executor = Executors.newSingleThreadExecutor();

		//<----------UNDOING--------------------->
		UndoIMTable = new ConcurrentHashMap<Integer,Stack<UndoIMLog>>();
		//</----------UNDOING--------------------->

		xidCounter = 1;
		callables = new HashSet<Callable<Integer>>();
		abrtdTxns = new ConcurrentHashMap<Integer, Object>();

		// How many threads do we want ?
		// This is a configurable value. Need to set it to optimal value.
		checkPointers = Executors.newFixedThreadPool(5); 

		callables.add(new TableWriter((Object)flightTable,"flightTable"));
		callables.add(new TableWriter((Object)carTable,"carTable"));
		callables.add(new TableWriter((Object)hotelTable,"hotelTable"));
		callables.add(new TableWriter((Object)reservationTable,"reservationTable"));
		callables.add(new TableWriter((Object)reservedflights,"reservedFlights"));

		checkAndCreateData();
		try{
			loadFiles();
		}
		catch(FileNotFoundException e){
			System.out.println("Cannot find file/s (Database probably does not exist only): " + e.getMessage());
		}
		System.out.println("Load files done/not done");		
		try{
			recover();
		}
		catch(FileNotFoundException e){
			System.out.println("Nothing to recover"+ e.getMessage());
		}
		System.out.println("Recovery done");

		//TODO : why this?? synchronized (shuttingDown) {		
		/*synchronized (stopAndWait) {
			stopAndWait = Boolean.valueOf(false);
			stopAndWait.notifyAll();
		}*/
		System.out.println("closing constructor");
	}

	public void setLogFile(String fileName){
		logFile = fileName;
		logWriter = new LogWriter(fileName);
	}

	public void isValidTrxn(int xid)
			throws InvalidTransactionException, TransactionAbortedException
			{
		//System.out.println("No of active transactions: " + activeTxns.size());
		//System.out.println(xid + ": " + activeTxns.get(xid));
		System.out.println("Current XID: " + xid);
		System.out.println(abrtdTxns.toString());
		if(abrtdTxns.contains(xid)){
			throw new TransactionAbortedException(xid,"");
		}
		if(activeTxns.get(xid) == null){
			throw new InvalidTransactionException(xid,"");
		}
		return ;

			}

	private void updateCheckPointVariables()
	{
		// If there is no shutdown , then checkpointing is over, this means we can reset the stopAndWait flag.
		committedTrxns.set(0);
		synchronized(HashSetEmpty)
		{
			HashSetEmpty.set(true);
		}
		
		synchronized(enteredTxnsCount)
		{
			enteredTxnsCount=0;
		}
		
		synchronized(stopAndWait)
		{
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


			if(tries==3)
			{
				System.out.println("FATAL ERROR: Unable to commit.killing system!!!");
				dieNow();
			}

			for(Future<Integer> future : futures){
				if(future.get()==1)
				{
					System.out.println("FATAL ERROR: aiayaoooo checkpoint failed da deei!!!");
					failed=true;
				}
			}

		}
		catch(InterruptedException e)
		{
			System.out.println(" dafaq woke me up ?"+e.getMessage());
		}
		catch(ExecutionException e)
		{
			System.out.println("dafaq?"+e.getMessage());

		}
		if(failed)checkPoint(tries+1);
		logWriter.flush();
		System.out.println("CHeckpoint END");
		return;
		//executorService.shutdown();
	}

	private void stopIncoming() throws RemoteException
	{
		synchronized(stopAndWait)
		{
			if(!stopAndWait.get()){
				stopAndWait.set(true);
			}
			else
				return;
			// {
			// 	//means stopandwait already raised due to some other condition.
			// 	//makes no difference , can only happen in case of shutdown/Cp or CP/shutdown.
			// 	// hence do nothing.
			// }
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

					System.out.println("dafaq woke me up ?"+e.getMessage());
					System.out.println(" value of HashSetEmpty"+HashSetEmpty.toString());
				}
			}
		}

		//do a checkpoint always. if shutdown flag is enabled, then also shutdown the system.
		//code for checkpointing

		checkPoint(0);
		RecoveryManager recoveryManager = new RecoveryManager(logFile);
		try{
			System.out.println("Doing cleanup");
			recoveryManager.deleteLogs();
			System.out.println("done cleanup");
		}
		catch(SecurityException e){
			System.out.println("Security permission issues: "+e.getMessage());
		}
		catch(FileNotFoundException e){
			System.out.println("PROBLEM WHILE DELETING LOGS");
		}
		catch(IllegalMonitorStateException e)
		{
				e.printStackTrace();e.getCause();
		}
		if(shuttingDown.get()>0)
			System.exit(0);
		
		System.out.println("about to update variables");
		updateCheckPointVariables();
		System.out.println("Stop incoming ends");
		return;
	}

	// TRANSACTION INTERFACE
	public int start()
			throws RemoteException {
		int temp;
		synchronized(enteredTxnsCount)
		{
			System.out.println("entering start==========");
			synchronized(stopAndWait)
			{
				while(stopAndWait.get())
				{
					try{
						System.out.println("waiting on stopAndWait");
						stopAndWait.wait();
					}
					catch(InterruptedException e)
					{

						System.out.println(" dafaq woke me up ?"+e.getMessage());
						System.out.println(" value of stopandwait"+stopAndWait.toString());
					}
				}
			}
			// do a checkpoint if atleast CPT transactions have entered, and atleast half of them have committed.
			if(enteredTxnsCount>=CHECKPOINT_TRIGGER && committedTrxns.get() >= (CHECKPOINT_TRIGGER/2))
			{
				stopIncoming(); //note here that the checkpointing is being done on a thread which has not been allocated a Xid yet.
				System.out.println("checkpointing....");
			}//else check if already some process is trying to stop incoming
			if(activeTxns.containsKey(xidCounter)){
				// HOW TO HANDLE THIS ?
				System.out.println("SHOULD NOT REACH: XID DUPLICATE");
			}
			enteredTxnsCount++;
			temp = xidCounter++;
		}

		synchronized(HashSetEmpty)
		{
			activeTxns.put(temp,DUMMY);
			System.out.println("tid assigned is "+temp);
			HashSetEmpty.set(false);
		}

		//<----------UNDOING--------------------->
		UndoIMTable.put(temp,new Stack<UndoIMLog>() );
		//</----------UNDOING--------------------->
		System.out.println("started succesfully");
		return (temp);
	}

	public void removeXID (int xid) throws InvalidTransactionException, TransactionAbortedException
	{
		isValidTrxn(xid);
		synchronized(activeTxns){
			System.out.println("About the remove entry from hashmap");
			activeTxns.remove(xid);
			System.out.println("Done removing from hashmap");
			if(activeTxns.size()==shuttingDown.get()){
				System.out.println("active transactions are virtually empty");
				HashSetEmpty.set(true);
			}
			System.out.println("Notifying");
			synchronized(HashSetEmpty){
				HashSetEmpty.notify();
			}
			System.out.println("Notified");
		}
		System.out.println("About the remove xid from UndoIMTable");
		UndoIMTable.remove(xid);
		System.out.println("Releasing all the locks");
		lockManager.unlockAll(xid);
		System.out.println("Returning from removeXID method");
		return;
	}


	public boolean commit(int xid)
			throws RemoteException, 
			TransactionAbortedException, 
			InvalidTransactionException {
		System.out.println("Committing");
		// When xid is removed from the hashset , see if the hashset becomes equal to the shuttingDown.get() value -
		// implies there are no more useful processes left. hence can shutdown the system.
		
		synchronized(DieBeforeCommit)
		{
			if(DieBeforeCommit)
			{
				dieNow();
			}
				
		}
		
		Future returnVal = executor.submit(new TransactionLogger(xid+" " + "COMMIT\n", logWriter));
		try
		{
			returnVal.get();
		}
		catch(Exception e)
		{
			System.out.println("Something hapened while retrieving value of atomic integer retunVal.Lets all zink about zees now"+e.getMessage());
		}
		logWriter.flush();
		synchronized(DieAfterCommit)
		{
			if(DieAfterCommit)
			{
				dieNow();
			}
				
		}
		removeXID(xid);
		System.out.println("Done commiting=======");
		return true;
	}

	//<----------UNDOING--------------------->
	public void performUndo(UndoIMLog entry)
	{

		switch(entry.tableName)
		{
		case FlightTable:
			if(entry.operation==insert)
			{
				System.out.println("insert in flight..undoing...");
				System.out.println(entry.Key);
				System.out.println("size of flighttable before undo"+flightTable.size());
				flightTable.remove(entry.Key);
				System.out.println("size of flighttable after undo"+flightTable.size());
			}
			else if(entry.operation == overWrite)
			{

				Flight oldData = (Flight)(entry.ObjPointer);
				Flight newData = flightTable.get(entry.Key);
				newData.copyFlight(oldData);
			}
			else if(entry.operation == delete)
			{
				flightTable.put(entry.Key,(Flight)(entry.ObjPointer));
			}
			break;
		case HotelTable:
			if(entry.operation==insert)
			{
				hotelTable.remove(entry.Key);
			}
			else if(entry.operation == overWrite)
			{

				Hotels oldData = (Hotels)(entry.ObjPointer);
				Hotels newData = hotelTable.get(entry.Key);
				newData.copyHotels(oldData);
			}
			else if(entry.operation == delete)//not required .. not going to happen..
			{
				hotelTable.put(entry.Key,(Hotels)(entry.ObjPointer));
			}
			break;
		case CarTable:
			if(entry.operation==insert)
			{
				carTable.remove(entry.Key);
			}
			else if(entry.operation == overWrite)
			{

				Car oldData = (Car)(entry.ObjPointer);
				Car newData = carTable.get(entry.Key);
				newData.copyCar(oldData);
			}
			else if(entry.operation == delete)
			{
				carTable.put(entry.Key,(Car)(entry.ObjPointer));
			}
			break;
		case ReservationTable:
			if(entry.operation==insert)
			{
				reservationTable.remove(entry.Key);
			}
			else if(entry.operation == partialInsert)
			{

				HashSet<Reservation> checkForFlights = reservationTable.get(entry.Key);
				checkForFlights.remove(entry.AuxKey);
			}
			else if(entry.operation == delete)
			{
				HashSet<Reservation> ref = (HashSet<Reservation>)(entry.ObjPointer);
				reservationTable.put(entry.Key,ref);
			}
			break;
		case Flights:
			if(entry.operation==insert)
			{
				reservedflights.remove(entry.Key);
			}
			else if(entry.operation == overWrite)
			{

				Integer oldData = (Integer)(entry.ObjPointer);
				Integer newData = (Integer)reservedflights.get(entry.Key);
				newData = oldData;
			}
			else if(entry.operation == delete)
			{
				//has to be decide based on kewals design.
			}
			break;
		default:
			System.out.println("should not freaking happen......");
			System.out.println(entry.tableName);
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
		
		try {
			isValidTrxn(xid);
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
				if(entry==null)
				{
					System.out.println("oh my god.... ! why the f$%^ is this null?");
				}
				performUndo(entry);
			}
			catch(Exception e)
			{
				retries--;
				System.out.println("retyring the aborttion of the last operation");
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
		System.out.println(" Aborted=======");
	
		try {
			removeXID(xid);
		} catch (TransactionAbortedException e) {
			System.out.println("");
		}
		return;
	}

	// ADMINISTRATIVE INTERFACE
	public boolean addFlight(int xid, String flightNum, int numSeats, int price) 
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {
				System.out.println("ResourceManager: entered addFlight");
		String lockString = "Flight."+flightNum;

		//Check if valid XID
		System.out.println("ResourceManager: about to check validity");
		isValidTrxn(xid);
		System.out.println("ResourceManager: done checking validity of transaction");

		try {
			if(lockManager.lock(xid, lockString, WRITE) == false){
				// Abort and then throw transaction aborted exception
				abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + flightNum);
			}
		} catch (DeadlockException e) {
			// Handle DeadLock !
			abort(xid);
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
		isValidTrxn(xid);

		try {
			if(lockManager.lock(xid, lockString, WRITE) == false){
				abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + flightNum);
			}
		} catch (DeadlockException e) {
			abort(xid);
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

		isValidTrxn(xid);
		try{
			if(location==null)
				return false;
			String lockString = "Hotels."+location;
			if(lockManager.lock(xid, lockString, WRITE) == false){
				abort(xid);
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
			abort(xid);
			e.printStackTrace();
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		catch( Exception e)	{
			abort(xid);
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
		isValidTrxn(xid);
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
			abort(xid);
			e.printStackTrace();
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		catch( Exception e)	{
			abort(xid);
			e.printStackTrace();
			throw new TransactionAbortedException(xid, "Aborted transaction because 'Other' Exception found: "+xid);

		}
	}


	public boolean addCars(int xid, String location, int numCars, int price) 
			throws RemoteException, 

			TransactionAbortedException,
			InvalidTransactionException {

		isValidTrxn(xid);		
		String lockString = "Cars."+location;
		StringBuilder logMsg = new StringBuilder("");
		try {
			if(lockManager.lock(xid, lockString, WRITE) == false){
				abort(xid);
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
			abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);

		} catch (Exception e) {
			e.printStackTrace();
			abort(xid);
			throw new TransactionAbortedException(xid, "Transaction aborted for unknown reasons" + "MSG: " + e.getMessage());
		}
		executor.execute(new VariableLogger(logMsg.toString(), logWriter));
		return true;
	}

	public boolean deleteCars(int xid, String location, int numCars) 
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {
		isValidTrxn(xid);
		String lockString = "Cars."+location;
		StringBuilder logMsg = new StringBuilder("");
		try {
			if(lockManager.lock(xid, lockString, WRITE) == false){
				abort(xid);
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
			abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		} catch (Exception e) {
			e.printStackTrace();
			abort(xid);
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
		isValidTrxn(xid);

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
			abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		catch( Exception e)	{
			e.printStackTrace();
			abort(xid);
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
				abort(xid);
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
						abort(xid);
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
						abort(xid);
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
						abort(xid);
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
			abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		catch( Exception e)	{
			e.printStackTrace();
			abort(xid);
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
		isValidTrxn(xid);

		if(flightNum == null)
			throw new InvalidTransactionException(xid, "message");

		// Acquiring read locks
		String lockString = "Flight."+flightNum;
		try {
			if(lockManager.lock(xid, lockString, READ) == false){
				abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}
		} catch (DeadlockException e) {
			e.printStackTrace();
			abort(xid);
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
		isValidTrxn(xid);

		if(flightNum == null)
			throw new InvalidTransactionException(xid, "message");

		// Acquiring read locks
		String lockString = "Flight."+flightNum;
		try {
			if(lockManager.lock(xid, lockString, READ) == false){
				abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}
		} catch (DeadlockException e) {
			e.printStackTrace();
			abort(xid);
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
		isValidTrxn(xid);

		if(location==null)
			return 0;
		try{
			String lockString = "Hotels."+location;
			if(lockManager.lock(xid, lockString, READ) == false){
				abort(xid);
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
			abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		catch( Exception e)	{
			e.printStackTrace();
			abort(xid);
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
		isValidTrxn(xid);
		if(location==null)
			return 0;
		try{
			String lockString = "Hotels."+location;
			if(lockManager.lock(xid, lockString, READ) == false){
				abort(xid);
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
			abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		catch( Exception e)	{
			e.printStackTrace();
			abort(xid);
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
		isValidTrxn(xid);
		String lockString = "Cars."+location;
		try {
			if(lockManager.lock(xid, lockString, READ) == false){
				abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}

			if(carTable.containsKey(location)){
				return carTable.get(location).getNumAvail();
			}else{
				return 0;
			}
		} catch (DeadlockException e) {
			e.printStackTrace();
			abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		} catch (Exception e) {
			e.printStackTrace();
			abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because 'Other' Exception found: "+xid);
		}
	}

	public int queryCarsPrice(int xid, String location)
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {
		isValidTrxn(xid);
		String lockString = "Cars."+location;
		try {
			if(lockManager.lock(xid, lockString, READ) == false){
				abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}

			if(carTable.containsKey(location)){
				return carTable.get(location).getPrice();
			}else{
				return 0;
			}
		} catch (DeadlockException e) {
			e.printStackTrace();
			abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		} catch (Exception e) {
			e.printStackTrace();
			abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because 'Other' Exception found: "+xid);
		}
	}

	public int queryCustomerBill(int xid, String custName)
			throws RemoteException, 
			TransactionAbortedException,
			InvalidTransactionException {

		if(custName==null)
			return 0;
		int customerBill = 0;
		try{
			// Acquire Lock
			String lockString = "Reservations." + custName;
			if(lockManager.lock(xid, lockString, READ) == false){
				abort(xid);
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
						abort(xid);
						throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
					}
					customerBill += flightTable.get(key).getPrice();
					break;
				case 2:
					lockString = "Hotels." + key;
					if(lockManager.lock(xid, lockString, READ) == false){
						abort(xid);
						throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
					}
					customerBill += hotelTable.get(key).getPrice();
					break;
				case 3:
					lockString = "Cars." + key;
					if(lockManager.lock(xid, lockString, READ) == false){
						abort(xid);
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
			abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		catch( Exception e)	{
			e.printStackTrace();
			abort(xid);
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
		isValidTrxn(xid);

		String lockString = "Flight." + flightNum;
		try {
			if(lockManager.lock(xid, lockString, WRITE) == false){
				abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString); 	
			}
			lockString = "Reservations."+custName;
			if(lockManager.lock(xid, lockString, WRITE) == false){
				abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);

			}
		} catch (DeadlockException e) {
			e.printStackTrace();
			abort(xid);
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
			logRec = new UndoIMLog(FlightTable,insert,null,flightNum,null);
			//<----------UNDOING--------------------->
		}
		else
		{

			//<----------UNDOING--------------------->
			Integer num=(Integer)reservedflights.get(flightNum);
			logRec = new UndoIMLog(FlightTable,overWrite,num,flightNum,null);
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

		isValidTrxn(xid);
		String lockString = "Cars." + location;
		try {
			if(lockManager.lock(xid, lockString, WRITE) == false){
				abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}
			lockString = "Reservations."+custName;
			if(lockManager.lock(xid, lockString, WRITE) == false){
				abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}
		} catch (DeadlockException e) {
			e.printStackTrace();
			abort(xid);
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
		isValidTrxn(xid);	
		try{
			String lockString = "Hotels." + location;

			if(lockManager.lock(xid, lockString, WRITE) == false){
				abort(xid);
				throw new TransactionAbortedException(xid, "Lock Manager returned false while acquiring lock on: " + lockString);
			}
			lockString = "Reservations."+custName;
			if(lockManager.lock(xid, lockString, WRITE) == false){
				abort(xid);
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
			abort(xid);
			throw new TransactionAbortedException(xid, "Aborted transaction because deadlock detected for XID: "+xid);
		}
		catch( Exception e)	{
			e.printStackTrace();
			abort(xid);
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
		synchronized(DieBeforeCommit)
		{
			DieBeforeCommit=DieBeforeCommit.valueOf(true);
		}
		return true;
	}

	public boolean dieAfterPointerSwitch() 
			throws RemoteException {
		synchronized(DieAfterCommit)
		{
			DieAfterCommit=DieAfterCommit.valueOf(true);
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
		boolean flag = recoveryManager.analyze();
		abrtdTxns = recoveryManager.getAbrtdTxns();
		xidCounter = recoveryManager.getMAXid() + 1;
		
		if(flag==false){
			System.out.println("No Need to recover");
			return;
		}
		System.out.println("After calling get aborted");
		System.out.println("ABorted Transactions");
		for(Integer s : abrtdTxns.keySet()){
			System.out.println(s);
		}
		
		System.out.println("Analyze phase done");
		if(recoveryManager.redo()==false){
			System.out.println("Failed during redo");
			return;
		}
		System.out.println("REDO phase done");
	}
}
