package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			//KVServer server = new KVServer(50000, 10, "FIFO");
			// server.clearStorage();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		System.out.println("Milestone 4 Testing Suite");
		
		// clientSuite.addTestSuite(ConnectionTest.class); //(Regression test)
		// clientSuite.addTestSuite(InteractionTest.class); //(Regression test) requires starting server at port 50000 and ECS at port 60000 separately 
		
		// clientSuite.addTestSuite(AdditionalTest.class); 
		// clientSuite.addTestSuite(AdditionalTestM1.class); //(Regression test) requires starting server at port 50000, and ECS at port 60000 and 60001 separately
		
		//clientSuite.addTestSuite(AdditionalTestM2.class); 
		//clientSuite.addTestSuite(PerformanceTestM1.class); 
		//clientSuite.addTestSuite(PerformanceTestM2.class); 
		// clientSuite.addTestSuite(AdditionalTestM3.class); 
		// clientSuite.addTestSuite(PerformanceTestM3.class); 
		// clientSuite.addTestSuite(PerformanceTestScaling.class); 
<<<<<<< HEAD

		// clientSuite.addTestSuite(AdditionalTestM4.class); 
		clientSuite.addTestSuite(PerformanceTestM4.class); 
=======
		clientSuite.addTestSuite(PerformanceTestSubscribe.class);
>>>>>>> a2648d4 (c)
		return clientSuite;
	}
	
}
