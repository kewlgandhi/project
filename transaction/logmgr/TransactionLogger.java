package transaction.logmgr;

import java.util.concurrent.Callable;

public class TransactionLogger implements Callable<Boolean> {

	private String logMsg;
	private LogWriter writer;

	public TransactionLogger(String logMsg, LogWriter writer){
		this.logMsg = logMsg;
		this.writer = writer;
	}

	@Override
	public Boolean call() throws Exception {
		writer.write(logMsg);
		return false;
	}
	

}
