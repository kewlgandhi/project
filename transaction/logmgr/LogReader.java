package transaction.logmgr;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class LogReader {

	private BufferedReader fr;
	private String fileName = "Redo";
	
	public LogReader(String fileName){
		this.fileName += fileName;
	}

	public void loadFile() throws FileNotFoundException{
		System.out.println("Trying to load undo redo logs");
		fr = new BufferedReader(new FileReader("./data/"+fileName));
		if(fr == null){
			System.out.println("fr is null");
		}
		System.out.println("Redo logs loaded: "+fileName);
	}

	public String nextLine(){
		String line = null;
		try {
			line = fr.readLine();
		} catch (IOException e) {
			System.out.println("Error in writing the logs "+fileName);
		}
		return line;
	}


	public void close(){
		try {
			fr.close();
		} catch (IOException e) {
			System.out.println("Error in closing the file "+fileName);
		}
	}

}
