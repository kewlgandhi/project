package transaction.recovery;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class TMRecoveryManager {

    public static final String logFile = "TM.log";
    private BufferedReader fr;
	private ConcurrentHashMap<Integer, Object> comtdTxns;
	private ConcurrentHashMap<Integer, Object> abrtdTxns;
	private ConcurrentHashMap<Integer, Object> ongngTxns;
	private ConcurrentHashMap<Integer, Object> prprdTxns;
    
	public void loadFile() throws FileNotFoundException{
		System.out.println("Trying to load TM logs");
		fr = new BufferedReader(new FileReader("./data/"+logFile));
		if(fr == null){
			System.out.println("fr is null");
		}
		System.out.println("TM Redo logs loaded: "+logFile);
	}

	public String nextLine(){
		String line = null;
		try {
			line = fr.readLine();
		} catch (IOException e) {
			System.out.println("Error in writing the TM logs "+logFile);
		}
		return line;
	}


	public void close(){
		try {
			fr.close();
		} catch (IOException e) {
			System.out.println("Error in closing the TM log file "+logFile);
		}
	}
	
	public TMRecoveryManager(){
		
	}
	
	public void analyzeTMlogs(){
		
	}
	
	public void recoverTM(){
		
	}
	
	
}
