import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RemoteException;
import java.rmi.registry.*;
import java.rmi.Naming;
import java.net.MalformedURLException;
import java.rmi.NotBoundException;

public class Server extends UnicastRemoteObject implements ServerIntf {  // ServerInterface
	private static int num_front=2; // initial number for front-tier
	private static int num_middle=2; // initial
	private static int operation=-1; // 0:master, 1:front, 2:middle
	private static int VM_id=-1;
	private static ServerLib SL;
	public static Cache cache = null;
	private static Cloud.DatabaseOps db = null;
	
	private static Hashtable<Integer, Integer> hash_IdOperation = new Hashtable<Integer, Integer>(); // 0:master, 1:front, 2:middle
	private static LinkedList<Cloud.FrontEndOps.Request> requestList = new LinkedList<Cloud.FrontEndOps.Request>();
	private static Map<String, String> cache_data = new ConcurrentHashMap<String, String>();


	public Server() throws Exception {
		super();
	}

	public static void main ( String args[] ) throws Exception {
		if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VM id>");
		String ip = args[0];
		int port = Integer.parseInt(args[1]);
		SL = new ServerLib( args[0], port );
		VM_id = Integer.parseInt(args[2]);

		// RMI connection
		Registry registry = LocateRegistry.getRegistry(port);
		ServerIntf master=null;
		try{
			Server server = new Server();
			if(VM_id==1){ registry.bind("Master", server); }
			else{ master = (ServerIntf) Naming.lookup("//localHost:"+ port + "/Master"); }
		}catch(Exception e) {
			System.err.println("Exception, Master RMI");
			e.printStackTrace();
		}

		// Master launch initial servers
		// Master is the VM has id equals to 1
		if(VM_id==1){
			cache = new Cache(SL);
			for(int i=0; i<num_middle; i++){ hash_IdOperation.put(SL.startVM(), 2); } //middle
			for(int i=0; i<num_front-1; i++){ hash_IdOperation.put(SL.startVM(), 1); } //front (inculde master)
			masterWork( master );
		}else{
			// Assign different tier to its work
			try{ operation = master.getOperation(VM_id); }
			catch(Exception e){ System.err.println("EXCEPTION, getOperation"); }
			if(operation==1){ frontWork( master ); }
			else if(operation==2){ middleWork( master , port); }
		}
	}

	// Master
	private static void masterWork( ServerIntf master ){
		Cloud.FrontEndOps.Request r;
		long cur, lastSampleTime=-1;
		int flg_initial=-9999;
		long lastScaleOut = flg_initial;
		int dropNum = 0;
		int addMid = 0;
		int sampleNum = 10;
		int sampleInterval = 100;
		int curNum = 0;
		int systemLoad = 0;
		int count_highSystemLoad=0, count_midSystemLoad=0, count_lightSystemLoad=0;
		int coldDownTime = 5000;

		// Start getting requests from load balancer
		SL.register_frontend();
		long startTime = System.currentTimeMillis();

		while(true){
			cur = System.currentTimeMillis();

			// Process front-tier or drop requests
			r = SL.getNextRequest();
			if( requestList.size() >= num_middle*1.5 ){
				SL.drop(r);
				dropNum++;
			} else {
				requestList.add(r);
			}

			// Calculate arrival rate
			if( cur-lastSampleTime > sampleInterval){
				lastSampleTime = cur;
				systemLoad += SL.getQueueLength() + requestList.size() + dropNum;
				curNum++;
			}

			// Calculate how many middle-tier need for this system load
			if( curNum == sampleNum){
				if(lastScaleOut==flg_initial){
					//First time to scale
					if( systemLoad > 70 ){ addMid = 11-num_middle;}
					else if( systemLoad > 50){ addMid = 7-num_middle;}
				}else{
					//Need to have 3 times and cool down time to scale out
					if( (cur-lastScaleOut)>coldDownTime ){
						if( systemLoad > 60 ){ addMid = 11-num_middle; count_highSystemLoad++;}
						else if( systemLoad > 50){ addMid = 9-num_middle; count_midSystemLoad++;}
						else if( systemLoad > 30){ addMid = 7-num_middle; count_lightSystemLoad++;}
						else{ count_highSystemLoad=0; count_midSystemLoad=0; count_lightSystemLoad=0;}
					}

					if(count_highSystemLoad>=3){ 
						addMid = 11-num_middle;
						count_highSystemLoad=0;
						count_midSystemLoad=0;
						count_lightSystemLoad=0;} 
					else if((count_highSystemLoad+count_midSystemLoad)>=3){ 
						addMid = 9-num_middle;
						count_highSystemLoad=0;
						count_midSystemLoad=0;
						count_lightSystemLoad=0;}
					else if((count_highSystemLoad+count_midSystemLoad+count_lightSystemLoad)>=3){
						addMid = 7-num_middle;
						count_highSystemLoad=0;
						count_midSystemLoad=0;
						count_lightSystemLoad=0;}
					else{ addMid=0; }
				}

				// Scale out middle
				if(addMid>0){ 
					lastScaleOut = cur;
				}
				for (int i=0; i<addMid; i++){
					hash_IdOperation.put(SL.startVM(), 2);
					num_middle++;
				}

				// Restart sample
				systemLoad = 0;
				curNum = 0;
				dropNum = 0;
			}
		}

	} 

	// Job for front-tier
	// Simply get request from balancer and put it into a queue located on master
	private static void frontWork( ServerIntf master ){
		Cloud.FrontEndOps.Request r=null;

		SL.register_frontend();
		while (true) {
			r = SL.getNextRequest();
			try{ master.add(r); }
			catch(Exception e){ System.err.println("EXCEPTION, add"); }
		}
	}

	// Job for middle-tier
	// 1. Process request in requestList
	// 2. If the VM idle for too long, termitated itself
	private static void middleWork( ServerIntf master , int port){
		long cur = System.currentTimeMillis();
		long prev = cur;
		long processInterval = 99999;
		long stop = -1;
		int scaleInCount=0;
		long intervalThresholdA=800, intervalThresholdB=1000;

		Cloud.FrontEndOps.Request r = null;

		try{
			db = master.getCache();
		}catch(Exception e){
			System.err.println("Exception, getCache");
		}

		while (true){
			try{ r = master.poll(); }
			catch(Exception e){ int a=1; }

			if(r!=null){
				SL.processRequest(r,db);
				prev = cur;
				cur = System.currentTimeMillis();
				processInterval = cur-prev;

				try{ 
					num_middle = master.getNumMiddle();
				}catch(Exception e){
					System.err.println("Exception, getNumMiddle");
				}

				if( (processInterval>intervalThresholdA && num_middle>3)||(processInterval>intervalThresholdB && num_middle==3)  ){
					scaleInCount++;
					if( scaleInCount==3 ){
						try{ master.middleScaleIn(VM_id); }
						catch(Exception e){ System.err.println("Exception, middleScaleIn"); }
						break;
					}
				} else {
					scaleInCount = 0;
				}
			}
		}
	}


	// ******* Master RMI functions ********
	// Return the VM's tier from its VM id
	public int getOperation(int VM_id) throws RemoteException{
		int op = -1;
		if(!hash_IdOperation.containsKey(VM_id)) { return op; }
		else{ op = hash_IdOperation.get(VM_id); }
		return op;
	}

	// Add a request into requestList
	public synchronized void add(Cloud.FrontEndOps.Request r) throws RemoteException{
		requestList.add(r);
	}

	// Poll a request from requestList and return it to a middle-tier
	public synchronized Cloud.FrontEndOps.Request poll() throws RemoteException{
		if(requestList.peek()==null) {return null;}
		return requestList.poll();
	}

	// Get number of middle-tier that is currently running
	public int getNumMiddle() throws RemoteException{
		return num_middle;
	}

	// Terminated a middle-tier base on its VM id
	public void middleScaleIn(int id) throws RemoteException{
		SL.endVM(id);
		num_middle--;
		requestList.remove(id);
	}

	// Get cache, return the cache to middle-tier
	public Cloud.DatabaseOps getCache() throws RemoteException {
		Cloud.DatabaseOps ca = cache;
		return ca;
	}

}

