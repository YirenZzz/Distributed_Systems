package shared.messages;

import java.io.Serializable;
import org.apache.log4j.Logger;

public class KVMessageObject implements KVMessage{
	private Logger logger = Logger.getRootLogger();
	private StatusType status;
	private String key;
	private String value;
	
	public KVMessageObject() {
	    status = null;
		value = null;
		key = null;
	}

	public KVMessageObject(StatusType status, String key, String value) {
	    this.status = status;
	    this.key = key;
	    this.value = value;
    }

	//constructor from String
	public KVMessageObject(String msg) {
		String[] tokens = msg.split("\\s+");
		String key = null;
		String val = null;

		if (tokens[0] == "FAILED") {
			key = tokens[1];
			for (int i = 2; i < tokens.length; i++) {
				key = key + " " + tokens[i];
			}
			
		} else {
			if (tokens.length > 1) {
				key = tokens[1];
			}
			if (tokens.length > 2) {
				val = tokens[2];
				int tokenIdx = 3;
				while (tokens.length > tokenIdx) {
					val = val + " " + tokens[tokenIdx];
					tokenIdx++;
				}
			}
		}

		//Convert from String to enum
		StatusType st = null;
		try {
			st = StatusType.valueOf(tokens[0]);
		} catch (Exception e) {
			logger.error("StatusType is not valid: " + e);
		}
	    this.status = st;
	    this.key = key;
	    this.value = val;
    }


	@Override
	public String getKey() {
		return this.key;
	}
	
	@Override
	public String getValue() {
		return this.value;
	}
	
	@Override
	public StatusType getStatus() {
		return this.status;
	}
}


