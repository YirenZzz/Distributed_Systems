package shared.messages;

public class HandleStrResponse {
	public enum NextInst {
		NOINST,
		SUCCESSORSERVERWRITELOCK,
		SHUTDOWNNODE,
		FINISHTRANSFERALLDATA,
		UPDATEREPLICA,
		REPLICAGET,
		ADDREPLICASENDKEYS,
		REMOVEREPLICADELKEYS,
		SHUTDOWNDETECTEDADDREPLICA,
		AFTERMETADATAUPDATE
	}

	public NextInst nextInst;
	public KVMessage responseMessage;
	public String nextInstParam;
	
	public HandleStrResponse(NextInst n, KVMessage r) {
	    this.nextInst = n;
		this.responseMessage = r;
		this.nextInstParam = null;
	}

	public HandleStrResponse(NextInst n, KVMessage r, String p) {
	    this.nextInst = n;
		this.responseMessage = r;
		this.nextInstParam = p;
	}

}


