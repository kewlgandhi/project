package transaction.recovery;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import transaction.logmgr.LogReader;
import transaction.bean.Car;
import transaction.bean.Flight;
import transaction.bean.Hotels;
import transaction.bean.Reservation;

public class RecoveryManager {
	RedoCar redoCar;
	RedoFlight redoFlight;
	RedoHotel redoHotel;
	RedoReservation redoReservation;
	RedoReservedFlights redoReservedFlights;
	
	private ConcurrentHashMap<Integer, Object> comtdTxns;
	private ConcurrentHashMap<Integer, Object> abrtdTxns;
	private ConcurrentHashMap<Integer, Object> ongngTxns;
	private ConcurrentHashMap<Integer, Object> prprdTxns;
	
	private final Object DUMMY = new Object();
	
	private int MAXid = 0;
	private String logFile;
	LogReader logReader;

	
	public ConcurrentHashMap<Integer, Object> getAbrtdTxns() {
		return abrtdTxns;
	}

	public ConcurrentHashMap<Integer, Object> getComtdTxns() {
		return comtdTxns;
	}
	
	public ConcurrentHashMap<Integer, Object> getPrprdTxns() {
		return prprdTxns;
	}
	
	public int getMAXid() {
		return MAXid;
	}
	
	public RecoveryManager(String fileName){
		logFile = fileName;
	}

	public RecoveryManager(ConcurrentHashMap<String, Flight> flightTable, ConcurrentHashMap<String, Car> carTable, ConcurrentHashMap<String, Hotels> hotelTable, ConcurrentHashMap<String, HashSet<Reservation>> reservationTable, ConcurrentHashMap<String,Integer> reservedflights, String fileName){
		redoCar = new RedoCar(carTable);
		redoFlight = new RedoFlight(flightTable);
		redoHotel = new RedoHotel(hotelTable);
		redoReservation = new RedoReservation(reservationTable);
		redoReservedFlights = new RedoReservedFlights(reservedflights);
		logFile = fileName;
		logReader = new LogReader(fileName);
	}

	public boolean analyze() throws FileNotFoundException{
		// Load Undo Redo Logs
		comtdTxns = new ConcurrentHashMap<Integer, Object>();
		abrtdTxns = new ConcurrentHashMap<Integer, Object>();
		ongngTxns = new ConcurrentHashMap<Integer, Object>();
		prprdTxns = new ConcurrentHashMap<Integer, Object>();
		logReader.loadFile();
		System.out.println("Loaded undo-redo log file");
		// Create HashSet of Committed Transactions
		String nextLine = logReader.nextLine();
		if(nextLine==null){
			System.out.println("File Empty, No recovery required !");
			return false;
		}

		while(nextLine != null){
			if(nextLine.contains("COMMIT")){
				String[] xid = nextLine.split(" ");
				int XID = Integer.parseInt(xid[0]);
				comtdTxns.put(XID, DUMMY);
				prprdTxns.remove(XID);
				MAXid = (XID>MAXid)?XID:MAXid;
			}
			else if(nextLine.contains("ABORT")){
				String[] xid = nextLine.split(" ");
				int XID = Integer.parseInt(xid[0]);
				abrtdTxns.put(XID,DUMMY);
				if(prprdTxns.contains(XID));
					prprdTxns.remove(XID);
				MAXid = (XID>MAXid)?XID:MAXid;
			}
			else if(nextLine.contains("PREPARE")){
				String[] xid = nextLine.split(" ");
				int XID = Integer.parseInt(xid[0]);
				ongngTxns.remove(XID);
				prprdTxns.put(XID, DUMMY);
			}
			else {
				String[] xid = nextLine.split("@#@");
				int XID = Integer.parseInt(xid[0]);
				ongngTxns.put(XID,DUMMY);
				MAXid = (XID>MAXid)?XID:MAXid;
			}
			nextLine = logReader.nextLine();
		}
		abrtdTxns.putAll(ongngTxns);
		if(comtdTxns.size()==0)return false;
		logReader.close();
		return true;
	}


	public boolean redo() throws FileNotFoundException{
		logReader.loadFile();
		String nextLine = logReader.nextLine();
		while(nextLine != null){
			// The Log is commit, abort or start
			if(!nextLine.contains("@#@")){
				nextLine = logReader.nextLine();
				continue;
			}
			String[] xid = nextLine.split("@#@");
			// The transaction is not committed. No need to redo(unod has handled it)
			if(!comtdTxns.contains(Integer.parseInt(xid[0]))){
				nextLine = logReader.nextLine();
				continue;
			}

			// PERFORM REDO
			System.out.println("LOG RECORD is : " + nextLine);
			if(xid[1].equals("Flights")){
				if(xid[3].equals("")){
					if(xid[4].equals("INSERT")){
						
						redoFlight.insert(xid[2]);
					}
					else if(xid[4].equals("DELETE")){
						redoFlight.delete(xid[2]);
					}
				}
				else{
					if(xid[3].equals("Price")){
						System.out.println("Updating the Price");
						redoFlight.updatePrice(xid[2],Integer.parseInt(xid[5]));
					}
					else if(xid[3].equals("NumAvail")){
						redoFlight.updateNumAvail(xid[2],Integer.parseInt(xid[5]));
					}
					else if(xid[3].equals("NumSeats")){
						redoFlight.updateNumSeats(xid[2],Integer.parseInt(xid[5]));
					}

				}
			}

			// REDO for Cars
			else if(xid[1].equals("Cars")){
				if(xid[3].equals("")){
					if(xid[4].equals("INSERT")){
						redoCar.insert(xid[2]);
					}
					else if(xid[4].equals("DELETE")){
						redoCar.delete(xid[2]);
					}
				}
				else{
					if(xid[3].equals("Price")){
						redoCar.updatePrice(xid[2],Integer.parseInt(xid[5]));
					}
					else if(xid[3].equals("NumAvail")){
						redoCar.updateNumAvail(xid[2],Integer.parseInt(xid[5]));
					}
					else if(xid[3].equals("NumCars")){
						redoCar.updateNumCars(xid[2],Integer.parseInt(xid[5]));
					}

				}
			}

			// REDO for Hotels
			else if(xid[1].equals("Rooms")){
				if(xid[3].equals("")){
					if(xid[4].equals("INSERT")){
						redoHotel.insert(xid[2]);
					}
					else if(xid[4].equals("DELETE")){
						redoHotel.delete(xid[2]);
					}
				}
				else{
					if(xid[3].equals("Price")){
						redoHotel.updatePrice(xid[2],Integer.parseInt(xid[5]));
					}
					else if(xid[3].equals("NumAvail")){
						redoHotel.updateNumAvail(xid[2],Integer.parseInt(xid[5]));
					}
					else if(xid[3].equals("NumRooms")){
						redoHotel.updateNumRooms(xid[2],Integer.parseInt(xid[5]));
					}

				}
			}

			// REDO for Reservations
			else if(xid[1].equals("Reservations")){
				if(xid[4].equals("INSERT")){
					redoReservation.insert(xid[2]);
				}
				else if(xid[4].equals("DELETE")){
					redoReservation.delete(xid[2]);
				}
				else if(xid[4].equals("UPDATE")){
					redoReservation.update(xid[2], xid[5]);
				}
			}

			else if(xid[1].equals("ReservedFlights")){
				if(xid[3].equals("")){
					if(xid[4].equals("INSERT")){
						redoReservedFlights.insert(xid[2]);
					}
					else if(xid[4].equals("DELETE")){
	
					}
				}
				else{
					if(xid[3].equals("NumReserved")){
						redoReservedFlights.updateNumReserved(xid[2],Integer.parseInt(xid[5]));
					}

				}
			}
			// Read Next line
			nextLine = logReader.nextLine();
		}
		logReader.close();
		return true;
	}

	public boolean deleteLogs() throws FileNotFoundException, SecurityException{
			File f = new File("./data/"+logFile); 
			if(f.exists()){
				f.delete();
			}
		return true;
	}

}
