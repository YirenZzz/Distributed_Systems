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


    public ECSNode(String name, String host, int port){
        this.name = name;
        this.host = host;
        this.port = port;
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

    public void setNodeHashRange(String lowHash, String highHash) {
        this.lowHash = lowHash;
        this.highHash = highHash;
    }
}