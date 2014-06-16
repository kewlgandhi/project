package transaction.recovery;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import transaction.WorkflowController;
import transaction.logmgr.LogReader;


//TODO
//CHANGE RECOVERYMANAGER CALL IN RMIMPL
// GETRMINSTANCE IN WC
public class TMRecoveryManager {

	private String logFile;
	LogReader logReader;
	private final Object DUMMY = new Object();
	private WorkflowController wc = null;

	private ConcurrentHashMap<Integer, Object> inprogressTxns;
	private ConcurrentHashMap<Integer, Object> initializedTxns;
	private ConcurrentHashMap<Integer, Object> preparedTxns;
	private ConcurrentHashMap<Integer, Object> committedTxns;
	private ConcurrentHashMap<Integer, Object> abortedTxns;
	private ConcurrentHashMap<Integer, Object> completedTxns;


	public TMRecoveryManager(String fileName, WorkflowController wc){
		logFile = fileName;
		logReader = new LogReader(logFile);
		this.wc = wc;
	}

	public void analyzeTMlogs(){
		// Load Undo Redo Logs
		inprogressTxns = new ConcurrentHashMap<Integer, Object>();
		initializedTxns = new ConcurrentHashMap<Integer, Object>();
		preparedTxns = new ConcurrentHashMap<Integer, Object>();
		committedTxns = new ConcurrentHashMap<Integer, Object>();
		abortedTxns = new ConcurrentHashMap<Integer, Object>();
		completedTxns = new ConcurrentHashMap<Integer, Object>();

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
			return;
		}

		while(nextLine != null){
			if(nextLine.contains("INSERT")){
				String[] xid = nextLine.split("@#@");
				int XID = Integer.parseInt(xid[0]);
				inprogressTxns.put(XID, DUMMY);
			}
			else if(nextLine.contains("STATUS")){
				String[] xid = nextLine.split("@#@");
				int XID = Integer.parseInt(xid[0]);
				inprogressTxns.remove(XID, DUMMY);
				State logState = Integer.parseInt(xid[3]);
			}

		}
	}

	public void recoverTM(){

	}


}
