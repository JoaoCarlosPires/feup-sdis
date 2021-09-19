package distributedbackup.service;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import distributedbackup.protocol.Message;
import distributedbackup.protocol.PeerCheck;
import distributedbackup.protocol.TrackerCheck;
import distributedbackup.protocol.TrackerDispatcher;
import distributedbackup.network.SSLHandler;

@SuppressWarnings("unchecked")
public class Tracker extends Node {

    private static Tracker tracker = null;
    public String dbpath;

    private Tracker() {
        super();
    }

    public static Tracker getInstance(){
        if (tracker == null)
            Tracker.tracker = new Tracker();
        return Tracker.tracker;
    }

    public void handleMessage(Message msg, InetSocketAddress remoteAddress){
        if (msg.getSenderID() == this.getId())
            return;
        this.threadPool.execute(new TrackerDispatcher(this, msg, remoteAddress));
    }

    public void save(){
        Path directory = Paths.get("data/tracker_" + this.getId());
        try {
            Files.createDirectories(directory);

            new File(this.dbpath).createNewFile();
        } catch (Exception e) {
            System.out.println("Failed to create database file, reason: " + e.getMessage());
            e.printStackTrace();
        }

        try {
            FileOutputStream fileOut = new FileOutputStream(dbpath);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this.sslHandler.getPeers());
            out.flush();
            out.close();
            fileOut.close();
        } catch (Exception e) {
            System.out.println("Failed to create tracker peer database file, reason: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        // java Tracker protocol    id       port     trackerIP  trackerPort trackerID
        //   -     -    args[0]   args[1]   args[2]     args[3]     args[4]  args[5]
        Tracker tracker = Tracker.getInstance();
        tracker.setProtocolVersion(args[0]);
        tracker.setId(Integer.parseInt(args[1]));
        tracker.setPort(Integer.parseInt(args[2]));
        tracker.initiateSSLListener("server");
        tracker.dbpath = "data/tracker_" + tracker.getId() + "/db";

         if (args.length > 4){
            int tracker_port = Integer.parseInt(args[4]);
            int tracker_id = Integer.parseInt(args[5]);
            InetSocketAddress trackerAddress = new InetSocketAddress(InetAddress.getByName(args[3]), tracker_port);

            String header[] = {tracker.getProtocolVersion(), "TREGISTER", String.valueOf(tracker.getId())};

            Message msg = new Message(header, null, tracker.getPort());
            SSLHandler.send(msg, trackerAddress);

            tracker.sslHandler.addTracker(tracker_id, args[3] ,tracker_port);
        } 


        File f = new File(tracker.dbpath);

        if (f.exists() && !f.isDirectory())
            tracker.load();

        // Check peer state
        tracker.threadPool.scheduleAtFixedRate(new PeerCheck(tracker), 0, 1, TimeUnit.SECONDS);
        tracker.threadPool.scheduleAtFixedRate(new TrackerCheck(tracker), 0, 1, TimeUnit.SECONDS);
    }

    public synchronized void load(){
        try {
            FileInputStream fileIn = new FileInputStream(this.dbpath);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            ConcurrentHashMap<?, ?> peers = (ConcurrentHashMap<?, ?>) in.readObject();
            this.sslHandler.setPeers((ConcurrentHashMap<Integer, InetSocketAddress>) peers);
            in.close();
            fileIn.close();
        } catch (Exception e) {
            System.out.println("Unable to load saved peers, reason: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("Previous state has been loaded successfully");
    }
}
