package transaction;

import java.rmi.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** 
 * Workflow Controller for the Distributed Travel Reservation System.
 * 
 * Description: toy implementation of the WC.  In the real
 * implementation, the WC should forward calls to either RM or TM,
 * instead of doing the things itself.
 */

public class WorkflowControllerImpl_sooi
    extends java.rmi.server.UnicastRemoteObject
    implements WorkflowController {

    protected int flightcounter, flightprice, carscounter, carsprice, roomscounter, roomsprice; 
    protected int xidCounter;
    private ConcurrentHashMap<Integer,Object> activeTxns;
    
    protected ResourceManagerImpl rmFlights = null;
    protected ResourceManagerImpl rmRooms = null;
    protected ResourceManagerImpl rmCars = null;
    protected ResourceManagerImpl rmCustomers = null;
    protected TransactionManagerImpl tm = null;

    public static void main(String args[]) {
	System.setSecurityManager(new RMISecurityManager());

	String rmiPort = System.getProperty("rmiPort");
	if (rmiPort == null) {
	    rmiPort = "";
	} else if (!rmiPort.equals("")) {
	    rmiPort = "//:" + rmiPort + "/";
	}

	try {
	    WorkflowControllerImpl obj = new WorkflowControllerImpl();
	    Naming.rebind(rmiPort + WorkflowController.RMIName, obj);
	    System.out.println("WC bound");
	}
	catch (Exception e) {
	    System.err.println("WC not bound:" + e);
	    System.exit(1);
	}
    }
    /**
     * Interface for the Workflow Controller of the Distributed Travel
     * Reservation System.
     * <p>
     * Failure reporting is done using two pieces, exceptions and boolean
     * return values.  Exceptions are used for systemy things - like
     * transactions that were forced to abort, or don't exist.  Return
     * values are used for operations that would affect the consistency of
     * the database, like the deletion of more cars than there are.
     * <p>
     * If there is a boolean return value and you're not sure how it would
     * be used in your implementation, ignore it.  We used boolean return
     * values in the interface generously to allow flexibility in
     * implementation.  But don't forget to return true when the operation
     * has succeeded.
     * <p>
     * All methods in the interface are declared to throw RemoteException.
     * This exception is thrown by the RMI system during a remote method
     * call to indicate that either a communication failure or a protocol
     * error has occurred. Your code will never have to directly throw
     * this exception, but any client code that you write must catch the
     * exception and take the appropriate action.
     */

    
    public WorkflowControllerImpl_sooi() throws RemoteException {
    	flightcounter = 0;
    	flightprice = 0;
    	carscounter = 0;
    	carsprice = 0;
    	roomscounter = 0;
    	roomsprice = 0;
    	flightprice = 0;

    	xidCounter = 1;

    	while (!reconnect()) {
    		// would be better to sleep a while
    	} 
    	
    	try{
    		activeTxns = tm.getActiveTxns();
    	}catch(Exception ex){
    		activeTxns = new ConcurrentHashMap<Integer, Object>();
    	}

    }

    /**
     * Start a new transaction, and return its transaction id.
     *
     * @return A unique transaction ID > 0.  Return <=0 if server is not accepting new transactions.
     *
     * @throws RemoteException on communications failure.
     */
    // TRANSACTION INTERFACE
    public int start()
	throws RemoteException {
    	    	
    	try{
    		int temp = tm.start();
    		activeTxns.put(temp, new Object());
    		return temp;
    	}
    	catch(RemoteException e)
    	{
    		return 0;
    	}
    	
    }
    /**
     * Commit transaction.
     *
     * @param xid id of transaction to be committed.
     * @return true on success, false on failure.
     *
     * @throws RemoteException on communications failure.
     * @throws TransactionAbortedException if transaction was aborted.
     * @throws InvalidTransactionException if transaction id is invalid.
     * */
    public boolean commit(int xid)
	throws RemoteException, 
	       TransactionAbortedException, 
	       InvalidTransactionException {
    	System.out.println("Calling commit  at WC");

    	/*try{
			return(tm.commit(xid));
    	}
    	catch(RemoteException e)
    	{
    		return false;
    	}*/
    	boolean returnVal;
    	try{
    		returnVal = tm.commit(xid);
    	}catch(Exception ex){
    		activeTxns.remove(xid);
    		throw ex;
    	}
    	return returnVal;

    }
    /**
     * Abort transaction.
     *
     * @param xid id of transaction to be aborted.
     *
     * @throws RemoteException on communications failure.
     * @throws InvalidTransactionException if transaction id is invalid.
     * */
    public void abort(int xid)
	throws RemoteException, 
               InvalidTransactionException {
    	boolean returnVal;
    	try{
    		returnVal = tm.abort(xid);
    	}catch(Exception ex){
    		activeTxns.remove(xid);
    		throw ex;
    	}
    	
    	
    }
    /**
     * Add seats to a flight.  In general this will be used to create
     * a new flight, but it should be possible to add seats to an
     * existing flight.  Adding to an existing flight should overwrite
     * the current price of the available seats.
     *
     * @param xid id of transaction.
     * @param flightNum flight number, cannot be null.
     * @param numSeats number of seats to be added to the flight.(>=0)
     * @param price price of each seat. If price < 0, don't overwrite the current price; leave price at 0 if price<0 for very first add for this flight.
     * @return true on success, false on failure. (flightNum==null; numSeats<0...)
     *
     * @throws RemoteException on communications failure.
     * @throws TransactionAbortedException if transaction was aborted.
     * @throws InvalidTransactionException if transaction id is invalid.
     */

    // ADMINISTRATIVE INTERFACE
    public boolean addFlight(int xid, String flightNum, int numSeats, int price) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try{
    		return(rmFlights.addFlight(xid, flightNum, numSeats, price));
    	}
    	catch(RemoteException e)
    	{
    		return 0;
    	}
    	
    }
    /**
     * Delete an entire flight.
     * Should fail if a customer has a reservation on this flight.
     *
     * @param xid id of transaction.
     * @param flightNum flight number, cannot be null.
     * @return true on success, false on failure. (flight doesn't exist;has reservations...)
     *
     * @throws RemoteException on communications failure.
     * @throws TransactionAbortedException if transaction was aborted.
     * @throws InvalidTransactionException if transaction id is invalid.
     */
    public boolean deleteFlight(int xid, String flightNum)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
       	try{
       		return(rmFlights.deleteFlight(xid, flightNum));
    	}
    	catch(RemoteException e)
    	{
    		return 0;
    	}
    	
	
    }
    /**
     * Add rooms to a location.
     * This should look a lot like addFlight, only keyed on a location
     * instead of a flight number.
     *
     * @return true on success, false on failure. (location==null; numRooms<0...)
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
     	try{
     		return(rmRooms.addRooms(xid, location, numRooms, price));
    	}
    	catch(RemoteException e)
    	{
    		return 0;
    	}
    }
    /**
     * Delete rooms from a location.
     * This subtracts from both the toal and the available room count
     * (rooms not allocated to a customer).  It should fail if it
     * would make the count of available rooms negative.
     *
     * @return true on success, false on failure. (location doesn't exist; numRooms<0; not enough available rooms...)
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
    	try{
    		return(rmRooms.deleteRooms(xid, location, numRooms));
    	}
    	catch(RemoteException e)
    	{
    		return 0;
    	}

    }
    /**
     * Add cars to a location.
     * Cars have the same semantics as hotels (see addRooms).
     *
     * @return true on success, false on failure.
     *
     * @throws RemoteException on communications failure.
     * @throws TransactionAbortedException if transaction was aborted.
     * @throws InvalidTransactionException if transaction id is invalid.
     *
     * @see #addRooms
     * @see #addFlight
     */
    public boolean addCars(int xid, String location, int numCars, int price) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try{
    		return(rmCars.addCars(xid, location, numCars, price));
    	}
    	catch(RemoteException e)
    	{
    		return 0;
    	}
   	
    }
    /**
     * Delete cars from a location.
     * Cars have the same semantics as hotels.
     *
     * @return true on success, false on failure.
     *
     * @throws RemoteException on communications failure.
     * @throws TransactionAbortedException if transaction was aborted.
     * @throws InvalidTransactionException if transaction id is invalid.
     *
     * @see #deleteRooms
     * @see #deleteFlight
     */
    public boolean deleteCars(int xid, String location, int numCars) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try{
    		return(rmCars.deleteCars(xid, location, numCars));
    	}
    	catch(RemoteException e)
    	{
    		return 0;
    	}
    	  
    }
    /**
     * Add a new customer to database.  Should return success if
     * customer already exists.
     *
     * @param xid id of transaction.
     * @param custName name of customer.
     * @return true on success, false on failure.
     *
     * @throws RemoteException on communications failure.
     * @throws TransactionAbortedException if transaction was aborted.
     * @throws InvalidTransactionException if transaction id is invalid.
     */
    public boolean newCustomer(int xid, String custName) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try{
    		return(rmCustomers.newCustomer(xid, custName));
    	}
    	catch(RemoteException e)
    	{
    		return 0;
    	}
	
    }
    /**
     * Delete this customer and un-reserve associated reservations.
     *
     * @param xid id of transaction.
     * @param custName name of customer.
     * @return true on success, false on failure. (custName==null or doesn't exist...)
     *
     * @throws RemoteException on communications failure.
     * @throws TransactionAbortedException if transaction was aborted.
     * @throws InvalidTransactionException if transaction id is invalid.
     */
    public boolean deleteCustomer(int xid, String custName) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try{
    		return(rmCustomers.deleteCustomer(xid, custName));
    	}
    	catch(RemoteException e)
    	{
    		return 0;
    	}
    	
    }


    // QUERY INTERFACE
//////////
/**
 * Return the number of empty seats on a flight.
 *
 * @param xid id of transaction.
 * @param flightNum flight number.
 * @return # empty seats on the flight. (-1 if flightNum==null or doesn't exist)
 *
 * @throws RemoteException on communications failure.
 * @throws TransactionAbortedException if transaction was aborted.
 * @throws InvalidTransactionException if transaction id is invalid.
 */
    public int queryFlight(int xid, String flightNum)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try{
    		return(rmFlights.queryFlight(xid, flightNum));
    	}
    	catch(RemoteException e)
    	{
    		return 0;
    	}
    	
    }
    /** Return the price of a seat on this flight. Return -1 if flightNum==null or doesn't exist.*/
    public int queryFlightPrice(int xid, String flightNum)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try{
    		return(rmFlights.queryFlightPrice(xid, flightNum));
    	}
    	catch(RemoteException e)
    	{
    		return 0;
    	}
    	
    }
    /** Return the number of rooms available at a location. */
    public int queryRooms(int xid, String location)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try{
    		return(rmRooms.queryRooms(xid, location));
    	}
    	catch(RemoteException e)
    	{
    		return 0;
    	}
    	
    }
    /** Return the price of rooms at this location. */
    public int queryRoomsPrice(int xid, String location)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try{
    		return (rmRooms.queryRoomsPrice(xid, location));
    	}
    	catch(RemoteException e)
    	{
    		return 0;
    	}
	
    }
    /** Return the number of cars available at a location. */
    public int queryCars(int xid, String location)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try{
    		return(rmCars.queryCars(xid, location));
    	}
    	catch(RemoteException e)
    	{
    		return 0;
    	}
    
    }
    /** Return the price of rental cars at this location. */
    public int queryCarsPrice(int xid, String location)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try{
    		return(rmCars.queryCarsPrice(xid, location));
    	}
    	catch(RemoteException e)
    	{
    		return 0;
    	}
    
    }
    /** Return the total price of all reservations held for a customer. Return -1 if custName==null or doesn't exist.*/
    public int queryCustomerBill(int xid, String custName)
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try{
    		return(rmCustomers.queryCustomerBill(xid, custName));
    	}
    	catch(RemoteException e)
    	{
    		return 0;
    	}
    	
    }


    // RESERVATION INTERFACE
    /**
     * Reserve a flight on behalf of this customer.
     *
     * @param xid id of transaction.
     * @param custName name of customer.
     * @param flightNum flight number.
     * @return true on success, false on failure. (cust or flight doesn't exist; no seats left...)
     *
     * @throws RemoteException on communications failure.
     * @throws TransactionAbortedException if transaction was aborted.
     * @throws InvalidTransactionException if transaction id is invalid.
     */
    public boolean reserveFlight(int xid, String custName, String flightNum) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try{
    		return(rmFlights.reserveFlight(xid, custName, flightNum));
    	}
    	catch(RemoteException e)
    	{
    		return false;
    	}
    	 return true;
    }
    /** Reserve a car for this customer at the specified location. */
    public boolean reserveCar(int xid, String custName, String location) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try{
    		return(rmCars.reserveCar(xid, custName, location));
    	}
    	catch(RemoteException e)
    	{
    		return false;
    	}
    return true;
    }
    /** Reserve a room for this customer at the specified location. */
    public boolean reserveRoom(int xid, String custName, String location) 
	throws RemoteException, 
	       TransactionAbortedException,
	       InvalidTransactionException {
    	try{
    		return(rmRooms.reserveRoom(xid, custName, location));
    	}
    	catch(RemoteException e)
    	{
    		return false;
    	}
    	catch(TransactionAbortedException e)
    	{
    		tm.abort(xid);
    		throw new TransactionAbortedException(Xid,"aborted at one of the locales . so undoing everything");
       	}
    	 return true;
    }
    /**
     * Reserve an entire itinerary on behalf of this customer.
     *
     * @param xid id of transaction.
     * @param custName name of customer.
     * @param flightNumList list of String flight numbers.
     * @param location location of car & hotel, if needed.
     * @param needCar whether itinerary includes a car reservation.
     * @param needRoom whether itinerary includes a hotel reservation.
     * @return true on success, false on failure. (Any needed flights/car/room doesn't exist or not available...)
     *
     * @throws RemoteException on communications failure.
     * @throws TransactionAbortedException if transaction was aborted.
     * @throws InvalidTransactionException if transaction id is invalid.
     */
    public boolean reserveItinerary(int xid, String custName, List flightNumList, String location, boolean needCar, boolean needRoom)
        throws RemoteException,
	TransactionAbortedException,
	InvalidTransactionException {
    	boolean result=true;
    	try{
    		
    		for(Object flightNum: flightNumList)
    					result&=rmFlights.reserveFlight(xid, custName,(String) flightNum);
    			
    		if(needCar)
    			result&=rmCars.reserveCar(xid, custName, location);
    		
    		if(needRoom)
    			result&=rmRooms.reserveRoom(xid, custName, location);
    	}
    	catch(RemoteException e)
    	{
    		return false;
    	}
    	catch(TransactionAbortedException e)
    	{
    		tm.abort(xid);
    		throw new TransactionAbortedException(Xid,"aborted at one of the locales . so undoing everything");
       	}
    	 return result;
    }

    // TECHNICAL/TESTING INTERFACE
    /**
     * If some component has died and was restarted, this function is
     * called to refresh the RMI references so that everybody can talk
     * to everybody else again.  Specifically, the WC should reconnect
     * to all other components, and each RM's reconnect() is called so
     * that the RM can reconnect to the TM.
     * <p>
     * This method is used for testing and is not part of a transaction.
     *
     * @return true on success, false on failure. (some component not up yet...)
     */
    public boolean reconnect()
	throws RemoteException {
	String rmiPort = System.getProperty("rmiPort");
	if (rmiPort == null) {
	    rmiPort = "";
	} else if (!rmiPort.equals("")) {
	    rmiPort = "//:" + rmiPort + "/";
	}

	try {
	    rmFlights =
		(ResourceManagerImpl)Naming.lookup(rmiPort +
				ResourceManager.RMINameFlights);
	    System.out.println("WC bound to RMFlights");
	    rmRooms =
		(ResourceManagerImpl)Naming.lookup(rmiPort +
				ResourceManager.RMINameRooms);
	    System.out.println("WC bound to RMRooms");
	    rmCars =
		(ResourceManagerImpl)Naming.lookup(rmiPort +
				ResourceManager.RMINameCars);
	    System.out.println("WC bound to RMCars");
	    rmCustomers =
		(ResourceManagerImpl)Naming.lookup(rmiPort +
				ResourceManager.RMINameCustomers);
	    System.out.println("WC bound to RMCustomers");
	    tm =
		(TransactionManagerImpl)Naming.lookup(rmiPort +
				TransactionManager.RMIName);
	    System.out.println("WC bound to TM");
	} 
	catch (Exception e) {
	    System.err.println("WC cannot bind to some component:" + e);
	    return false;
	}

	try {
	    if (rmFlights.reconnect() && rmRooms.reconnect() &&
		rmCars.reconnect() && rmCustomers.reconnect()) {
		return true;
	    }
	} catch (Exception e) {
	    System.err.println("Some RM cannot reconnect:" + e);
	    return false;
	}

	return false;
    }
    /**
     * Kill the component immediately.  Used to simulate a system
     * failure such as a power outage.
     * <p>
     * This method is used for testing and is not part of a transaction.
     *
     * @param who which component to kill; must be "TM", "RMFlights", "RMRooms", "RMCars", "RMCustomers", "WC", or "ALL" (which kills all 6 in that order).
     * @return true on success, false on failure.
     */
    public boolean dieNow(String who)
	throws RemoteException {
	if (who.equals(TransactionManager.RMIName) ||
	    who.equals("ALL")) {
	    try {
		tm.dieNow();
	    } catch (RemoteException e) {}
	}
	if (who.equals(ResourceManager.RMINameFlights) ||
	    who.equals("ALL")) {
	    try {
		rmFlights.dieNow();
	    } catch (RemoteException e) {}
	}
	if (who.equals(ResourceManager.RMINameRooms) ||
	    who.equals("ALL")) {
	    try {
		rmRooms.dieNow();
	    } catch (RemoteException e) {}
	}
	if (who.equals(ResourceManager.RMINameCars) ||
	    who.equals("ALL")) {
	    try {
		rmCars.dieNow();
	    } catch (RemoteException e) {}
	}
	if (who.equals(ResourceManager.RMINameCustomers) ||
	    who.equals("ALL")) {
	    try {
		rmCustomers.dieNow();
	    } catch (RemoteException e) {}
	}
	if (who.equals(WorkflowController.RMIName) ||
	    who.equals("ALL")) {
	    System.exit(1);
	}
	return true;
    }
    /**
     * Sets a flag so that the RM fails after the next enlist()
     * operation.  That is, the RM immediately dies on return of the
     * enlist() call it made to the TM, before it could fulfil the
     * client's query/reservation request.
     * <p>
     * This method is used for testing and is not part of a transaction.
     *
     * @param who which RM to kill; must be "RMFlights", "RMRooms", "RMCars", or "RMCustomers".
     * @return true on success, false on failure.
     */
    public boolean dieRMAfterEnlist(String who)
	throws RemoteException {
    	 	if (who.equals(ResourceManager.RMINameFlights) ){
    		    try {
    			rmFlights.setDieAfterEnlist(true);
    		    } catch (RemoteException e) {
    		    	return false;
    		    }
    		    return true;
    		}
    		if (who.equals(ResourceManager.RMINameRooms) ) {
    		    try {
    			rmRooms.setDieAfterEnlist(true);;
    		    } catch (RemoteException e) {
    		    	return false;
    		    }
    		    return true;
    		}
    		if (who.equals(ResourceManager.RMINameCars) ) {
    		    try {
    			rmCars.setDieAfterEnlist(true);
    		    } catch (RemoteException e) {
    		    	return false;
    		    }
    		    return true;
    		}
    		if (who.equals(ResourceManager.RMINameCustomers) ) {
    		    try {
    			rmCustomers.setDieAfterEnlist(true);
    		    } catch (RemoteException e) {
    		    	return false;
    		    }
    		    return true;
    		}
       	
    } /**
     * Sets a flag so that the RM fails when it next tries to prepare,
     * but before it gets a chance to save the update list to disk.
     * <p>
     * This method is used for testing and is not part of a transaction.
     *
     * @param who which RM to kill; must be "RMFlights", "RMRooms", "RMCars", or "RMCustomers".
     * @return true on success, false on failure.
     */
    public boolean dieRMBeforePrepare(String who)
	throws RemoteException {
    	if (who.equals(ResourceManager.RMINameFlights) ){
		    try {
			rmFlights.setDieBeforePrepare(true);
		    } catch (RemoteException e) {
		    	return false;
		    }
		    return true;
		}
		if (who.equals(ResourceManager.RMINameRooms) ) {
		    try {
			rmRooms.setDieBeforePrepare(true);
		    } catch (RemoteException e) {
		    	return false;
		    }
		    return true;
		}
		if (who.equals(ResourceManager.RMINameCars) ) {
		    try {
			rmCars.setDieBeforePrepare(true);
		    } catch (RemoteException e) {
		    	return false;
		    }
		    return true;
		}
		if (who.equals(ResourceManager.RMINameCustomers) ) {
		    try {
			rmCustomers.setDieBeforePrepare(true);
		    } catch (RemoteException e) {
		    	return false;
		    }
		    return true;
		}
    }
    /**
     * Sets a flag so that the RM fails when it next tries to prepare:
     * after it has entered the prepared state, but just before it
     * could reply "prepared" to the TM.
     * <p>
     * This method is used for testing and is not part of a transaction.
     *
     * @param who which RM to kill; must be "RMFlights", "RMRooms", "RMCars", or "RMCustomers".
     * @return true on success, false on failure.
     */
    public boolean dieRMAfterPrepare(String who)
	throws RemoteException {
    	if (who.equals(ResourceManager.RMINameFlights) ){
		    try {
			rmFlights.setDieAfterPrepare(true);
		    } catch (RemoteException e) {
		    	return false;
		    }
		    return true;
		}
		if (who.equals(ResourceManager.RMINameRooms) ) {
		    try {
			rmRooms.setDieAfterPrepare(true);
		    } catch (RemoteException e) {
		    	return false;
		    }
		    return true;
		}
		if (who.equals(ResourceManager.RMINameCars) ) {
		    try {
			rmCars.setDieAfterPrepare(true);
		    } catch (RemoteException e) {
		    	return false;
		    }
		    return true;
		}
		if (who.equals(ResourceManager.RMINameCustomers) ) {
		    try {
			rmCustomers.setDieAfterPrepare(true);
		    } catch (RemoteException e) {
		    	return false;
		    }
		    return true;
		}
    }
    /**
     * Sets a flag so that the TM fails after it has received
     * "prepared" messages from all RMs, but before it can log
     * "committed".
     * <p>
     * This method is used for testing and is not part of a transaction.
     *
     * @return true on success, false on failure.
     */
    public boolean dieTMBeforeCommit()
	throws RemoteException {
    	
    	try{
    		tm.setDieTMbeforeCommit(true);
    	}
    	catch(RemoteException e)
    	{
    		return false;
    	}
    	return true;
    }
    /**
     * Sets a flag so that the TM fails right after it logs
     * "committed".
     * <p>
     * This method is used for testing and is not part of a transaction.
     *
     * @return true on success, false on failure.
     */
    public boolean dieTMAfterCommit()
	throws RemoteException {
    	try{
    		tm.setDieTMafterCommit(true);
    	}
    	catch(RemoteException e)
    	{
    		return false;
    	}
    	return true;
    }
    /**
     * Sets a flag so that the RM fails when it is told by the TM to
     * commit, by before it could actually change the database content
     * (i.e., die at beginning of the commit() function called by TM).
     * <p>
     * This method is used for testing and is not part of a transaction.
     *
     * @param who which RM to kill; must be "RMFlights", "RMRooms", "RMCars", or "RMCustomers".
     * @return true on success, false on failure.
     */
    public boolean dieRMBeforeCommit(String who)
	throws RemoteException {
    	if (who.equals(ResourceManager.RMINameFlights) ){
		    try {
			rmFlights.setDieBeforeCommit(true);
		    } catch (RemoteException e) {
		    	return false;
		    }
		    return true;
		}
		if (who.equals(ResourceManager.RMINameRooms) ) {
		    try {
			rmRooms.setDieBeforeCommit(true);
		    } catch (RemoteException e) {
		    	return false;
		    }
		    return true;
		}
		if (who.equals(ResourceManager.RMINameCars) ) {
		    try {
			rmCars.setDieBeforeCommit(true);
		    } catch (RemoteException e) {
		    	return false;
		    }
		    return true;
		}
		if (who.equals(ResourceManager.RMINameCustomers) ) {
		    try {
			rmCustomers.setDieBeforeCommit(true);
		    } catch (RemoteException e) {
		    	return false;
		    }
		    return true;
		}
    }
    /**
     * Sets a flag so that the RM fails when it is told by the TM to
     * abort, by before it could actually do anything.  (i.e., die at
     * beginning of the abort() function called by TM).
     * <p>
     * This method is used for testing and is not part of a transaction.
     *
     * @param who which RM to kill; must be "RMFlights", "RMRooms", "RMCars", or "RMCustomers".
     * @return true on success, false on failure.
     */
    public boolean dieRMBeforeAbort(String who)
	throws RemoteException {
    	if (who.equals(ResourceManager.RMINameFlights) ){
		    try {
			rmFlights.setDieBeforeAbort(true);
		    } catch (RemoteException e) {
		    	return false;
		    }
		    return true;
		}
		if (who.equals(ResourceManager.RMINameRooms) ) {
		    try {
			rmRooms.setDieBeforeAbort(true);
		    } catch (RemoteException e) {
		    	return false;
		    }
		    return true;
		}
		if (who.equals(ResourceManager.RMINameCars) ) {
		    try {
			rmCars.setDieBeforeAbort(true);
		    } catch (RemoteException e) {
		    	return false;
		    }
		    return true;
		}
		if (who.equals(ResourceManager.RMINameCustomers) ) {
		    try {
			rmCustomers.setDieBeforeAbort(true);
		    } catch (RemoteException e) {
		    	return false;
		    }
		    return true;
		}
    }
}
