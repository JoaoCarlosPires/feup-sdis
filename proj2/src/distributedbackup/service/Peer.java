package distributedbackup.service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import distributedbackup.filesystem.LocalFile;
import distributedbackup.filesystem.LocalFileInfo;
import distributedbackup.filesystem.State;
import distributedbackup.network.SSLHandler;
import distributedbackup.protocol.DeleteInitiator;
import distributedbackup.protocol.Dispatcher;
import distributedbackup.protocol.Message;


public class Peer extends Node implements InitiatorPeer {


    public ScheduledExecutorService threadPool;
    
    public State state;

    public Peer(String protocolVersion, int peerID, int port) throws IOException {
        super();
        this.setId(peerID);
        this.setPort(port);
        this.setProtocolVersion(protocolVersion);
        
        this.threadPool = new ScheduledThreadPoolExecutor(32);

        this.initiateSSLListener("server");
        
        if (State.hasExistingDatabase(this.getId()))
            this.state = State.load(this.getId());
        else this.state = new State(this.getId());

        System.out.println("Peer ready");
    }

    public void handleMessage(Message msg, InetSocketAddress remoteAddress){
        if (msg.getSenderID() == this.getId())
            return;
        this.threadPool.execute(new Dispatcher(this, msg, remoteAddress));
    }

    public boolean sendTrackerRegister(int tracker_id){
        String header[] = {this.protocolVersion, "REGISTER", String.valueOf(getId())};
        Message msg = new Message(header, null, this.getPort());
        return SSLHandler.send(msg, this.sslHandler.getTrackers().get(tracker_id));
    }


    public static void main(String[] args) throws IOException{
        if (args.length < 6) {
            System.out.println("Usage: <protocol_version> <peer_ID> <port> <tracker_ip> <tracker_port> <tracker_id>");
            System.exit(-1);
        }

        int peerID = Integer.parseInt(args[1]);
        int port = Integer.parseInt(args[2]);
        int tracker_id = Integer.parseInt(args[5]);
        int tracker_port = Integer.parseInt(args[4]);
        
        Peer peer = new Peer(args[0], peerID, port);
        peer.sslHandler.addTracker(tracker_id, args[3], tracker_port);

        InitiatorPeer initiatorPeer = (InitiatorPeer) UnicastRemoteObject.exportObject(peer, 0);
        Registry registry = LocateRegistry.getRegistry();
        registry.rebind(args[1], initiatorPeer);
        

        if (!peer.sendTrackerRegister(tracker_id)){
            System.out.println("Failed to connect to target tracker with hostname '" + args[3] + "' port '" + tracker_port +"'");
            System.exit(-1);
        }
    }


    @Override
    public void backup(String pathname, int replicationDegree) {
        LocalFile file = new LocalFile(pathname, replicationDegree, this);
        file.backup();
    }

    @Override
    public void restore(String pathname) {
        LocalFileInfo fileInfo = this.state.getLocalFileByPathname(pathname);
        LocalFile file = new LocalFile(fileInfo, this);
        file.restore();
    }

    @Override
    public void delete(String pathname) {
        LocalFileInfo file = this.state.getLocalFileByPathname(pathname);
        if (file == null) {
            System.out.println("This file doesnt belong to this peer!");
            return;
        }
        file.clearChunks();
        this.state.removeLocalFile(file);

        this.threadPool.execute(new DeleteInitiator(this, file.getFileID()));

        System.out.println("Removed file: " + pathname);
        this.state.save();
    }

    @Override
    public void reclaim(int maxDiskSpace) {
        this.state.reclaim(maxDiskSpace, this);
    }

    @Override
    public void getState(){
        System.out.println("---------------------------------");
        System.out.println("LOCAL FILES BACKED UP");
        this.state.printLocalFiles();
        System.out.println("---------------------------------");
        System.out.println("CHUNKS STORED LOCALLY");
        this.state.printStoredChunks();
        this.state.printSpace();
        System.out.println("---------------------------------");
    }


    public String getProtocolVersion() {
        return protocolVersion;
    }
}
