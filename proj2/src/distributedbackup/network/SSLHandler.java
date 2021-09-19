package distributedbackup.network;

import distributedbackup.protocol.Message;
import distributedbackup.service.Node;

import javax.net.ssl.*;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

public class SSLHandler implements Runnable {
    public int port;
    private SSLServerSocket sslSocket;
    private Node node;
    protected ConcurrentHashMap<Integer, InetSocketAddress> trackers = new ConcurrentHashMap<>();
    protected ConcurrentHashMap<Integer, InetSocketAddress> peers = new ConcurrentHashMap<>();
    protected final ConcurrentHashMap<Integer, Long> timestamps = new ConcurrentHashMap<>();

    public SSLHandler(String key, int port, Node node){
        this.port = port;
        System.setProperty("javax.net.ssl.keyStore", "keys/" + key + ".keys");
        System.setProperty("javax.net.ssl.keyStorePassword", "123456");
        System.setProperty("javax.net.ssl.trustStore", "keys/truststore");
        System.setProperty("javax.net.ssl.trustStorePassword", "123456");
        this.sslSocket = this.startSSocket();
        this.node = node;
    }

    public void addPeer(int peerID, String ip, int port){
        if (!peers.containsKey(peerID)){
            try{
            InetSocketAddress add = new InetSocketAddress(InetAddress.getByName(ip), port);
            peers.put(peerID, add);
            }
            catch(Exception e){
                System.out.println("Failed to add peer");
                System.out.println(e.getMessage());
            }
        }
    }

    public boolean removePeer(int peerID) {
        if (this.peers.remove(peerID) != null) {
            this.timestamps.remove(peerID);
            System.out.println("Removed peer with id: " + peerID);
            return true;
        }
        return false;
    }

    public void addTracker(int trackerID, String ip, int port){
        if (!trackers.containsKey(trackerID)){
            try{
                InetSocketAddress add = new InetSocketAddress(InetAddress.getByName(ip), port);
                trackers.put(trackerID, add);
            }
            catch(Exception e){
                System.out.println("Failed to add Tracker");
                System.out.println(e.getMessage());
            }
        }
    }

    public void removeTracker(int trackerID) {
        this.trackers.remove(trackerID);
        this.timestamps.remove(trackerID);
        System.out.println("Removed tracker with id: " + trackerID);
    }

    public ConcurrentHashMap<Integer, InetSocketAddress> getTrackers() {
        return trackers;
    }

    private SSLServerSocket startSSocket(){

        SSLServerSocketFactory sslServerSocketFactory = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
        SSLServerSocket sslServerSocket ;

        try{
            sslServerSocket = (SSLServerSocket) sslServerSocketFactory.createServerSocket(port);
            sslServerSocket.setNeedClientAuth(true);
            sslServerSocket.setEnabledProtocols(sslServerSocket.getSupportedProtocols());
            sslServerSocket.setEnabledCipherSuites(sslServerSocket.getSupportedCipherSuites());
        }catch(IOException e){
            throw new RuntimeException("Could not open port : " + this.port , e);
        }

        return sslServerSocket;
    }

    public static boolean send(Message msg, InetSocketAddress serverAdd){
        try{
            SSLSocket ssocket = null;

            SSLSocketFactory factory = (SSLSocketFactory)SSLSocketFactory.getDefault();

            ssocket = (SSLSocket) factory.createSocket();
            ssocket.connect(serverAdd, 5000);
            ssocket.setEnabledCipherSuites(ssocket.getSupportedCipherSuites());
            ObjectOutputStream out_stream = new ObjectOutputStream(ssocket.getOutputStream());
            out_stream.writeObject(msg);
            ssocket.close();
        } catch (Exception e){
           return false;
        }
        return true;
    }

    public void sendToAllPeers(Message msg){
        for (InetSocketAddress peer : peers.values()){
            SSLHandler.send(msg, peer);
        }
    }

    public void sendToAllTrackers(Message msg){
        for (InetSocketAddress tracker : trackers.values()){
            SSLHandler.send(msg, tracker);
        }
    }


    public ConcurrentHashMap<Integer, Long> getTimestamps() {
        return timestamps;
    }

    public ConcurrentHashMap<Integer, InetSocketAddress> getPeers() {
        return peers;
    }

    public void setPeers(ConcurrentHashMap<Integer, InetSocketAddress> peers) {
        this.peers = peers;
    }

    @Override
    public void run() {
        System.out.println("Now listening");
        while(true) {
            try {
                SSLSocket socket = (SSLSocket) this.sslSocket.accept();

                // open streams
                ObjectInputStream input_stream = new ObjectInputStream(socket.getInputStream());

                Message msg = (Message) input_stream.readObject();

                timestamps.put(msg.getSenderID(), System.currentTimeMillis());

                node.handleMessage(msg, (InetSocketAddress)socket.getRemoteSocketAddress());
                // close streams
                input_stream.close();
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
