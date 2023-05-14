package ecs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Properties;
import org.apache.log4j.Logger;

import shared.communication.CommClient;
import shared.messages.*;


/*
 * todo: write node information (as java config) to file, iterate through the file
 * end node failure information to ECSClient using socket
 * In node shutdown, ECSClient removes node information from file first
 * Acquire read write lock (initialized in ECS, initialized as parameter in FailureDetector)
 * 
 * In ECSserver, add handler for shutdown
 * 
 */

public class FailureDetector {
    private String ecsIP;
    private int ecsPort;
    private String nodeConfigFileName; //updated during addNode
    private int checkInterval;
    private ReentrantReadWriteLock fileLock;
    private Logger logger;

    /*
     * For sending messages to servers 
     */
    private class CheckServerConnection implements Runnable {
        private String serverAddr;
        private Properties serverProps;
        private String nodeConfigFileName;
        private CommClient ecsClient;
        private Logger logger;


        public CheckServerConnection(String addr, Properties serverProps, String fileName, CommClient ecsClient, Logger logger) {
            this.serverAddr = addr;
            this.serverProps = serverProps;
            this.nodeConfigFileName = fileName;
            this.ecsClient = ecsClient;
            this.logger = logger;
        }

        public void run() {
            String[] arrServerAddr = serverAddr.split(":");
            CommClient client = new CommClient(arrServerAddr[0], Integer.parseInt(arrServerAddr[1]));
            
            try{
                client.connect();
                client.disconnect();

            } catch (Exception e) {
                logger.info("FailureDetector exception in connecting to " + serverAddr + ": " + e);
                serverProps.remove(serverAddr);
                
                //update the config file
                try {
                    serverProps.store(new FileOutputStream(nodeConfigFileName), "updated by FailureDetector");
                    
                    //send message to ECS to handle shutdown
                    String strECSMsg = "nodeshutdowndetected " + serverAddr;
                    ecsClient.sendMessage(new TextMessage(strECSMsg));
                    TextMessage ecsResp = ecsClient.receiveMessage();
                    logger.info("FailureDetector received: " + ecsResp.getMsg());

                } catch (IOException ex) {
                    logger.error("FailureDetector error: " + ex); //error storing to file or sending message to ecs
                }
            }

            
        }
    }



    public FailureDetector(String ip, int pt, String n, int c, ReentrantReadWriteLock f) {
        ecsIP = ip;
        ecsPort = pt;
        nodeConfigFileName = n;
        checkInterval = c;
        fileLock = f;
        logger = Logger.getRootLogger();
    }
    
    public void startChecking() {    
        
        Runnable r = new Runnable() {
            @Override
            public void run() {
                Properties serverProps = new Properties();//todo: create this file in ECS if does not exist
                CommClient ecsClient = new CommClient(ecsIP, ecsPort);
                try {
                    ecsClient.connect();
                } catch (Exception e) {
                    logger.error("FailureDetector is unable to connect to ECS");
                    System.exit(1);
                }
                
                while (true) {
                    try {
                        Thread.sleep(checkInterval);
                    } catch (InterruptedException e) {
                        logger.warn("FailureDetector interrupted: " + e);
                    }

                    fileLock.writeLock().lock(); //to ensure consistency

                    try (FileInputStream file = new FileInputStream(nodeConfigFileName)) {
                        
                        serverProps.load(file);
                        Set<String> setServers = serverProps.stringPropertyNames();

                        //for logging
                        String strServers = "";
                        for (String k : setServers) {
                            strServers = strServers + k + " "; 
                        }
                        logger.info("FailureDetector checking servers: " + strServers);
                        
                        //iterate through the current list of servers to check connection
                        for (String k : setServers) {
                            logger.info(k);

                            //start a new thread for each server
                            CheckServerConnection curServerConnection = new CheckServerConnection(k, serverProps, nodeConfigFileName, ecsClient, logger);
                            Thread curThread = new Thread(curServerConnection);
                            curThread.start();
                        }

                    } catch (IOException e) {
                        logger.error("FailureDetector error loading property file: " + e);
                    } finally {
                        fileLock.writeLock().unlock();
                    }
                }
            }
        };

        Thread t = new Thread(r);
        t.start();
    }

}

