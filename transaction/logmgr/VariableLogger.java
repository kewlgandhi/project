package transaction.logmgr;

public class VariableLogger implements Runnable {
	
	private String logMsg;
	private LogWriter writer;
	
	public VariableLogger(String logMsg, LogWriter writer){
		this.logMsg = logMsg;
		this.writer = writer;
	}

	@Override
	public void run() {
		//Write the message to the file.
		writer.write(logMsg);
		
	}

}
