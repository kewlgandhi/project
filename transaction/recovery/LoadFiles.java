package transaction.recovery;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import java.io.FileNotFoundException;
import transaction.bean.Car;
import transaction.bean.Flight;
import transaction.bean.Hotels;
import transaction.bean.Reservation;
import transaction.bean.TableReader;

public class LoadFiles {

	private ExecutorService restoreService ;
	private Set<Callable<Integer>> callables;
	private TableReader flightTR;
	private TableReader carTR;
	private TableReader hotelTR;
	private TableReader reservationTR;
	private TableReader reservedflightsTR;


	public LoadFiles(ExecutorService service){
		restoreService = service;
	}
	
	public void loadSetup(){
		callables = new HashSet<Callable<Integer>>();
		restoreService = Executors.newFixedThreadPool(5);

		flightTR = new TableReader("flightTable");
		carTR = new TableReader("carTable");
		hotelTR = new TableReader("hotelTable");
		reservationTR = new TableReader("reservationTable");
		reservedflightsTR = new TableReader("reservedFlights");

		callables.add(flightTR);
		callables.add(carTR);
		callables.add(hotelTR);
		callables.add(reservationTR);
		callables.add(reservedflightsTR);
		
	}

	public boolean load(int nTries) throws FileNotFoundException{

		boolean result = true;
		List<Future<Integer>> futures;
		try {
			futures = restoreService.invokeAll(callables);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		if(nTries==3)
		{
			System.out.println("Cannot load files :: 3 retries done");
			return false;
			// Kill system : Invoke dieNow in calling code
		}

		for(Future<Integer> future : futures){
			try {
				if(future.get() == 1){
					System.out.println("Load Attempt: "+nTries+" Failed");
					result = false;
				}else if(future.get() == 2){
					throw new FileNotFoundException("file not found exception thrown again by load method");
				}
			} catch (InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		if(result == false)
			result = load(nTries+1);
		return result;
	}
	
	public TableReader getTR(String fileName){
		if(fileName.equals("reservedFlights"))
			return reservedflightsTR;
		else if(fileName.equals("reservationTable"))
			return reservationTR;
		else if(fileName.equals("flightTable"))
			return flightTR;
		else if(fileName.equals("carTable"))
			return carTR;
		else if(fileName.equals("hotelTable"))
			return hotelTR;
		else
			System.out.println("Cannot find " + fileName);
		
		return null;
	}
	
}


