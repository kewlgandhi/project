package transaction.logmgr;

import java.io.FileWriter;
import java.io.IOException;

public class LogWriter {
	
	private String fileName = "Redo";
	private FileWriter fw;

	
	public LogWriter(String fileName){
		this.fileName += fileName;
	}
	
	//TODO : where are we calling this??
	public synchronized void loadFile(){
		System.out.println("Creating Redo logs for "+ fileName);
		try {
			fw = new FileWriter("./data/"+fileName);
		} catch (IOException e) {
			System.out.println("Error creating Redo log file "+ fileName);
			e.printStackTrace();
		}
	}
	
	public void write(String msg){
		try {
			fw.write(msg);
		} catch (IOException e) {
			System.out.println("Error in writing the logs");
		}
	}
	
	public void flush(){
		try {
			fw.flush();
			System.out.println("Flushed the disk logs");
		} catch (IOException e) {
			System.out.println("Error in flushing the log file");
		} catch (Exception e){
			System.out.println("Should not come here "+e);
		}
	}
	
	public void close(){
		try {
			fw.close();
		} catch (IOException e) {
			System.out.println("Error in closing the file");
		}
	}
}
