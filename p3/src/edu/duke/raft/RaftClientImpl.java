package edu.duke.raft;

import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.net.MalformedURLException;

public class RaftClientImpl extends UnicastRemoteObject {
    private int mID;
    private int port;
    private int counter;

    public RaftClientImpl(int clientID) throws RemoteException {
        this.mID = clientID;
        this.counter = 0;
    }

    private String getRmiUrl(int serverID, int serverPort) {
        return "rmi://localhost:" + serverPort + "/S" + serverID;
    }

    public void writeEntry(int serverID, int serverPort) {
        String item = "Client " + this.mID + ": Item " + this.counter;
        this.counter ++;

        (new Thread(new Runnable() {
            public void run() {
                String url = this.getRmiUrl(serverID, serverPort);
                try {
                    RaftServer server = (RaftServer) Naming.lookup(url);
                    server.receive(item);
                } catch (NotBoundException nbe) {
                    System.out.println("NotBoundException in writeEntry("+item+")");
                    throw nbe;
                } catch (RemoteException re) {
                    System.out.println("RemoteException in writeEntry(" + item + ")");
                    throw re;
                }
            }
        })).start();
    }

    


}
