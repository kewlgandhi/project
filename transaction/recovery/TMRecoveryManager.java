package transaction.recovery;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.MaximizeAction;

import transaction.ResourceManager;
import transaction.WorkflowController;
import transaction.bean.State;
import transaction.bean.TransactionDetails;
import transaction.logmgr.LogReader;


//TODO
//CHANGE RECOVERYMANAGER CALL IN RMIMPL
// GETRMINSTANCE IN WC
public class TMRecoveryManager {

	private String logFile;
	LogReader logReader;
	private final Object DUMMY = new Object();
	private WorkflowController wc;
	private ConcurrentHashMap<Integer, Object> inprogressTxns;
	private ConcurrentHashMap<Integer, Object> initializedTxns;
	private ConcurrentHashMap<Integer, Object> preparedTxns;
	private ConcurrentHashMap<Integer, Object> committedTxns;
	private ConcurrentHashMap<Integer, Object> abortedTxns;
	private ResourceManager rmFlights;
	private ResourceManager rmCars;
	private ResourceManager rmRooms;
	private ResourceManager rmCustomers;
	Map<Integer, TransactionDetails> transactions;
	int maxXID = 0;

	public TMRecoveryManager(String fileName){
		logFile = fileName;
		logReader = new LogReader(logFile);
	}

	public boolean analyze(){
		// Load Undo Redo Logs
		inprogressTxns = new ConcurrentHashMap<Integer, Object>();
		initializedTxns = new ConcurrentHashMap<Integer, Object>();
		preparedTxns = new ConcurrentHashMap<Integer, Object>();
		committedTxns = new ConcurrentHashMap<Integer, Object>();
		abortedTxns = new ConcurrentHashMap<Integer, Object>();

		try {
			logReader.loadFile();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("RedoTmLog not found in analyze Tmlog");
			e.printStackTrace();
		}
		System.out.println("Loaded undo-redo log file");
		// Create HashSet of Committed Transactions
		String nextLine = logReader.nextLine();
		if(nextLine==null){
			System.out.println("File Empty, No recovery required !");
			return false;
		}

		while(nextLine != null){
			if(nextLine.contains("INSERT")){
				String[] xid = nextLine.split("@#@");
				int XID = Integer.parseInt(xid[0]);
				inprogressTxns.put(XID, DUMMY);
				maxXID = ((maxXID > XID) ? maxXID : XID);
			}
			else if(nextLine.contains("STATUS")){
				String[] xid = nextLine.split("@#@");
				int XID = Integer.parseInt(xid[0]);
				inprogressTxns.remove(XID, DUMMY);
				if(xid[3].equals("1")){
					//Initialized 
					initializedTxns.put(XID, DUMMY);
				}
				else if(xid[3].equals("2")){
					//Prepared
					initializedTxns.remove(XID);
					preparedTxns.put(XID, DUMMY);
				}
				else if(xid[3].equals("3")){
					//Committed
					preparedTxns.remove(XID);
					committedTxns.put(XID, DUMMY);
				}
				else if(xid[3].equals("4")){
					//Aborted
					preparedTxns.remove(XID);
					abortedTxns.put(XID, DUMMY);
				}
			}
			nextLine = logReader.nextLine();
		}
		if(initializedTxns.size()==0 && preparedTxns.size()==0)
			return false;

		return true;
	}


	public boolean redo(){
		try {
			logReader.loadFile();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		transactions = new HashMap<Integer, TransactionDetails>();
		String nextLine = logReader.nextLine();
		while(nextLine != null){
			String[] xid = nextLine.split("@#@");
			int XID = Integer.parseInt(xid[0]);
			if(!initializedTxns.containsKey(XID) && !preparedTxns.containsKey(XID)){
				nextLine = logReader.nextLine();
				continue;
			}

			if(xid[2].equals("INSERT")){
				TransactionDetails details = new TransactionDetails(XID);	

				if(initializedTxns.containsKey(XID)){
					details.setStatus(State.INITIALIZED);
				}
				else{
					details.setStatus(State.PREPARED);
				}

				transactions.put(XID, details);
			}

			if(xid[2].equals("ADD")){
				TransactionDetails details = transactions.get(XID);
				ResourceManager rm = getInstance(xid[3]);
				details.addToRmList(rm);
			}
			nextLine = logReader.nextLine();
		}
		return true;
	}

	private ResourceManager getInstance(String string) {
		return null;
	}

	public int getMaxXID(){
		return maxXID;
	}

	public Map<Integer, TransactionDetails> getTransactions() {
		return transactions;
	}


}
