package transaction.bean;

import java.util.ArrayList;
import java.util.List;

import transaction.ResourceManagerImpl;

public class TransactionDetails {
	
	
	private int xid;
	private List<ResourceManagerImpl> rmList;
	private State status;
	
	public TransactionDetails(int xid){
		this.xid = xid;
		rmList = new ArrayList<ResourceManagerImpl>();
	}
	
	public void setStatus(State status){
		this.status = status;
	}
	
	public State getStatus() {
		return status;
	}

	public List<ResourceManagerImpl> getRmList() {
		return rmList;
	}

	public void setRmList(List<ResourceManagerImpl> rmList) {
		this.rmList = rmList;
	}

	public void addToRmList(ResourceManagerImpl rm){
		this.rmList.add(rm);
	}
	
	public boolean rmListContains(ResourceManagerImpl rm){
		return this.rmList.contains(rm);
	}

}
