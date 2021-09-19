package distributedbackup.service;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class TestApp implements Runnable {

    private final InitiatorPeer initiatorPeer;
    private final String peerAP;
    private final String subProtocol;
    private String opnd1;
    private String opnd2;

    public TestApp(String peerAP, String subProtocol) throws IOException, NotBoundException {
        Registry registry = LocateRegistry.getRegistry("localhost");
        initiatorPeer = (InitiatorPeer) registry.lookup(peerAP);
        this.peerAP = peerAP;
        this.subProtocol = subProtocol;
    }

    public void setOpnd1(String opnd1) {
        this.opnd1 = opnd1;
    }

    public void setOpnd2(String opnd2) {
        this.opnd2 = opnd2;
    }

    public static void main(String[] args) throws IOException, NotBoundException {
        if (args.length < 2 || args.length > 4) {
            System.out.println("Usage: java TestApp <peer_ap> <sub_protocol> <opnd_1> <opnd_2>");
            System.exit(-1);
        }

        TestApp testApp = new TestApp(args[0], args[1]);
        if (args.length > 2)
            testApp.setOpnd1(args[2]);
        if (args.length > 3)
            testApp.setOpnd2(args[3]);

        new Thread(testApp).start();
    }

    @Override
    public void run() {

        switch (this.subProtocol){
            case "BACKUP":{
                try {
                    this.initiatorPeer.backup(opnd1, Integer.parseInt(opnd2));
                } catch(RemoteException e){
                    System.out.println(e.getMessage());
                }
                break;
            }

            case "STATE":{
                try {
                this.initiatorPeer.getState();
                } catch(RemoteException e){
                    System.out.println(e.getMessage());
                }
                break;
            }

            case "RESTORE":{
                try {
                    this.initiatorPeer.restore(opnd1);
                } catch(RemoteException e){
                    System.out.println(e.getMessage());
                }
                break;
            }

            case "DELETE":{
                try {
                    this.initiatorPeer.delete(opnd1);
                } catch(RemoteException e){
                    System.out.println(e.getMessage());
                }
                break;
            }

            case "RECLAIM":{
                try {
                    this.initiatorPeer.reclaim(Integer.parseInt(opnd1));
                } catch(RemoteException e){
                    System.out.println(e.getMessage());
                }
                break;
            }

            default: break;
        }
    }
}
