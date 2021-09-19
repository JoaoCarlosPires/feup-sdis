package distributedbackup.protocol;

import java.net.InetSocketAddress;
import java.util.Map;

import distributedbackup.network.SSLHandler;
import distributedbackup.service.Tracker;

public class TrackerDispatcher implements Runnable {

    private final Tracker tracker;
    private final Message msg;
    private final InetSocketAddress senderAddress;

    public TrackerDispatcher(Tracker tracker, Message msg, InetSocketAddress senderAddress){
        this.tracker = tracker;
        this.msg = msg;
        this.senderAddress = senderAddress;
    }

    @Override
    public void run(){
        

        switch (msg.getMessageType()){
            case "REGISTER":{
                String remoteIP = this.senderAddress.getAddress().toString();
                tracker.sslHandler.addPeer(msg.getSenderID(), remoteIP.substring(remoteIP.lastIndexOf('/') + 1), msg.getPort());
                for (Map.Entry<Integer, InetSocketAddress> peer : tracker.sslHandler.getPeers().entrySet()){
                    if (!peer.getKey().equals(msg.getSenderID())){
                        this.sendNewPeerMessage(peer.getKey(), msg.getSenderID());
                        this.sendNewPeerMessage(msg.getSenderID(), peer.getKey());
                    }
                }

                for (Map.Entry<Integer, InetSocketAddress> tracker : tracker.sslHandler.getTrackers().entrySet()){
                    this.sendNewPeerMessageToTracker(msg.getSenderID(), tracker.getKey());
                    this.sendNewTrackerMessageToPeer(tracker.getKey(), msg.getSenderID());
                }

                System.out.println(this.msg);
                break;
            }
            case "TREGISTER":{
                String remoteIP = this.senderAddress.getAddress().toString();
                tracker.sslHandler.addTracker(msg.getSenderID(), remoteIP.substring(remoteIP.lastIndexOf('/') + 1), msg.getPort());
                for (Map.Entry<Integer, InetSocketAddress> tracker : tracker.sslHandler.getTrackers().entrySet()){
                    if (!tracker.getKey().equals(msg.getSenderID())){
                        this.sendNewTrackerMessage(tracker.getKey(), msg.getSenderID());
                        this.sendNewTrackerMessage(msg.getSenderID(), tracker.getKey());
                    }
                }

                for (Map.Entry<Integer, InetSocketAddress> peer : tracker.sslHandler.getPeers().entrySet()) {
                    this.sendNewPeerMessageToTracker(peer.getKey(), msg.getSenderID());
                }

                System.out.println(this.msg);
                break;
            }
            case "NEWTRACKER":{
                this.newTrackerHandler();
                System.out.println(this.msg);
                break;
            }
            case "NEWPEER":{
                this.newPeerHandler();
                System.out.println(this.msg);
                break;
            }
            case "REMPEER":{
                if (this.removePeerHandler())
                    System.out.println(this.msg);
                break;
            }
            case "ALIVE":{
                this.aliveHandler();
                break;
            }
            default: break;
        }
    }

    public void aliveHandler(){
        String[] header = new String[]{this.tracker.getProtocolVersion(), "OK", String.valueOf(this.tracker.getId())};
        Message msg = new Message(header, this.tracker.getPort());
        SSLHandler.send(msg, this.tracker.sslHandler.getTrackers().get(this.msg.getSenderID()));
    }

    public boolean removePeerHandler(){
        return this.tracker.sslHandler.removePeer(this.msg.getPort());
    }

    public void newTrackerHandler(){
        this.tracker.sslHandler.addTracker(this.msg.getSenderID(), this.msg.getPeerIP(), this.msg.getPeerPort());
        System.out.println("Added Tracker with IP " + this.msg.getPeerIP() + " and port " + this.msg.getPeerPort());
    }

    public void newPeerHandler(){
        this.tracker.sslHandler.addPeer(this.msg.getSenderID(), this.msg.getPeerIP(), this.msg.getPeerPort());
        System.out.println("Added Peer with IP " + this.msg.getPeerIP() + " and port " + this.msg.getPeerPort());
    }

    public void sendNewPeerMessage(int newPeerID, int targetPeerID){
        InetSocketAddress newPeerAddress = tracker.sslHandler.getPeers().get(newPeerID);
        InetSocketAddress targetPeerAddress = tracker.sslHandler.getPeers().get(targetPeerID);

        String header[] = {tracker.getProtocolVersion(), "NEWPEER", String.valueOf(newPeerID)};
        Message msg = new Message(header, null, newPeerAddress);
        SSLHandler.send(msg, targetPeerAddress);
    }

    public void sendNewTrackerMessageToPeer(int newTrackerID, int targetPeerID){
        InetSocketAddress newPeerAddress = tracker.sslHandler.getTrackers().get(newTrackerID);
        InetSocketAddress targetPeerAddress = tracker.sslHandler.getPeers().get(targetPeerID);

        String header[] = {tracker.getProtocolVersion(), "NEWTRACKER", String.valueOf(newTrackerID)};
        Message msg = new Message(header, null, newPeerAddress);
        SSLHandler.send(msg, targetPeerAddress);
    }

    public void sendNewPeerMessageToTracker(int newPeerID, int targetTrackerID){
        InetSocketAddress newPeerAddress = tracker.sslHandler.getPeers().get(newPeerID);
        InetSocketAddress targetPeerAddress = tracker.sslHandler.getTrackers().get(targetTrackerID);

        String header[] = {tracker.getProtocolVersion(), "NEWPEER", String.valueOf(newPeerID)};
        Message msg = new Message(header, null, newPeerAddress);
        SSLHandler.send(msg, targetPeerAddress);
    }

    public void sendNewTrackerMessage(int newTrackerID, int targetPeerID){
        InetSocketAddress newPeerAddress = tracker.sslHandler.getTrackers().get(newTrackerID);
        InetSocketAddress targetPeerAddress = tracker.sslHandler.getTrackers().get(targetPeerID);

        String header[] = {tracker.getProtocolVersion(), "NEWTRACKER", String.valueOf(newTrackerID)};
        Message msg = new Message(header, null, newPeerAddress);
        SSLHandler.send(msg, targetPeerAddress);
    }
}
