package shared.messages;

public interface KVMessage {
	
	public enum StatusType {
		CONNECTED, 
		DISCONNECTED, 
		CONNECTION_LOST,
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, 	/* Delete - request successful */
		FAILED,
		SERVER_STOPPED,
		SERVER_WRITE_LOCK,
		SERVER_NOT_RESPONSIBLE,
		KEYRANGE_SUCCESS,
		ADDNODEACK_SUCCESS,
		WRITELOCK_SUCCESS,
		TRANSFERDATAINVOKE_SUCCESS,
		TRANSFERDATA_SUCCESS,
		TRANSFERDATACOMPLETE_SUCCESS,
		METADATAUPDATE_SUCCESS,
		WRITELOCKRELEASE_SUCCESS,
		NODESHUDOWN_SUCCESS,
		ADDREPLICA_ACK,
		REMOVEREPLICA_ACK,
		NODESHUTDOWNDETECTED_ACK,
		KEYRANGE_READ_SUCCESS,
		SUB_SUCCESS,
		SUB_ERROR,
		SUB_INFO_RECEIVED,
		UNSUB_SUCCESS,
		UNSUB_ERROR,
		GETALLKEYS_SUCCESS,
		GETALLKEYS_ERROR,
		BEGINTX_SUCCESS,
		TRANSACTION_ERROR,
		COMMITTX_SUCCESS,
		ROLLED_BACK
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();	
}


