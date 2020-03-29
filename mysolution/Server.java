/* Sample code for basic Server */

public class Server {
	public static void main ( String args[] ) throws Exception {
		if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VM id>");
		ServerLib SL = new ServerLib( args[0], Integer.parseInt(args[1]) );
		int VM_id = Integer.parseInt(args[2]);
		
		// register with load balancer so requests are sent to this server
		SL.register_frontend();

		float time = SL.getTime();
		int n = 0;
		if(time==6){ n=2; }
		else if (time==8) { n=3; }
		else if (time==19) { n=4; }

		if(VM_id==1){
			for(int i=0; i<n; i++){
				SL.startVM();
			}
		}

		// main loop
		while (true) {
			Cloud.FrontEndOps.Request r = SL.getNextRequest();
			SL.processRequest( r );
		}
	}
}

