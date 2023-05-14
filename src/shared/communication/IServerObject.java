package shared.communication;

import shared.messages.HandleStrResponse;
import shared.messages.HandleStrResponse.NextInst;
import shared.messages.KVMessage;


/*
 * This is the interface that user of CommServer should implement
 */
public interface IServerObject {
    /**
     * Handle the message 
     * @return  cache size
     */
    public HandleStrResponse handleStrMsg(String strMsg);

    /**
     * Subsequent instructions
     * @return  cache size
     */
    public void handleNextInst(NextInst inst, String strMsg);


}
