package ecs;

import java.util.Map;
import java.util.Collection;


public class ECSNode implements IECSNode {
    private String name;
    private String host;
    private int port;
    private String lowHash;
    private String highHash;
    public String cacheStrategy;
    public Integer cacheSize;

    //stores the two servers the current server is replica for 
    //(can be the server itself, need to check for this case when using
    private IECSNode[] replicaFor; 


    public ECSNode(String name, String host, int port){
        this.name = name;
        this.host = host;
        this.port = port;
        this.replicaFor = new IECSNode[2];
    }
    
    @Override
    public String getNodeName() {
        return this.name;
    }

    @Override
    public String getNodeHost() {
        return this.host;
    }

    @Override
    public int getNodePort() {
        return this.port;
    }

    @Override
    public String[] getNodeHashRange() {
        String[] hashRange = new String[2];
        hashRange[0] = this.lowHash;
        hashRange[1] = this.highHash;
        return hashRange;
    }

    @Override
    public void setNodeHashRange(String lowHash, String highHash) {
        this.lowHash = lowHash;
        this.highHash = highHash;
    }

    @Override
    public IECSNode[] getReplicaInfo() {
        return this.replicaFor;
    }

    @Override
    public void setReplicaInfo(IECSNode[] newRepInfo) {
        this.replicaFor = newRepInfo;
    }

    @Override
    public void setReplicaInfoByIdx (int idx, IECSNode newRep) {
        this.replicaFor[idx] = newRep;
    }
}