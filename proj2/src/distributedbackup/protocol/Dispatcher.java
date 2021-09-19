package distributedbackup.protocol;

import distributedbackup.filesystem.Chunk;
import distributedbackup.filesystem.LocalChunk;
import distributedbackup.filesystem.StoredChunk;
import distributedbackup.network.SSLHandler;
import distributedbackup.service.Peer;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Comparator;
import java.util.Random;

public class Dispatcher implements Runnable {

    private final Peer peer;
    private final Message msg;
    private final InetSocketAddress senderAddress;

    public Dispatcher(Peer peer, Message msg, InetSocketAddress remoteAddress){
        this.peer = peer;
        this.msg = msg;
        this.senderAddress = remoteAddress;
    }

    @Override
    public void run(){

        switch(this.msg.getMessageType()){
            case "PUTCHUNK":{
                this.putChunkHandler();
                System.out.println(this.msg);
                break;
            }
            case "STORED":{
                this.storedHandler();
                System.out.println(this.msg);
                break;
            }
            case "DELETE":{
                this.deleteHandler();
                System.out.println(this.msg);
                break;
            }
            case "CHUNK":{
                this.chunkHandler();
                System.out.println(this.msg);
                break;
            }
            case "GETCHUNK":{
                this.getChunkHandler();
                System.out.println(this.msg);
                break;
            }
            case "REMOVED":{
                this.removedHandler();
                System.out.println(this.msg);
                break;
            }
            case "NEWPEER":{
                this.newPeerHandler();
                System.out.println(this.msg);
                break;
            }
            case "ALIVE":{
                try {
                    this.aliveHandler();
                }
                catch(Exception e){
                    System.out.println(e.getMessage());
                }
                break;
            }
            case "REMPEER":{
                if (this.removePeerHandler())
                    System.out.println(this.msg);
                break;
            }
            default:{
                System.out.println("Unknown message type");
                break;
            }
        }
    }

    public void newPeerHandler(){
        this.peer.sslHandler.addPeer(this.msg.getSenderID(), this.msg.getPeerIP(), this.msg.getPeerPort());
        System.out.println("Added peer with IP " + this.msg.getPeerIP() + " and port " + this.msg.getPeerPort());
    }



    public void storedHandler(){
        Chunk chunk = new Chunk(this.msg);

        if (this.peer.state.hasStoredChunk(chunk)) {
            this.peer.state.incPerceivedRepDegree(chunk);
            this.peer.state.save();
        }
        else if (this.peer.state.hasLocalFile(chunk.getFileID())){
            this.peer.state.getLocalChunk(chunk).incPerceivedRepDegree();
            this.peer.state.save();
        }
    }

    public void chunkHandler(){

        StoredChunk storedChunk = this.peer.state.getStoredChunk(this.msg);
        if (storedChunk != null){

            storedChunk.gotAnswer = true;
        }
        else{
            LocalChunk localChunk = this.peer.state.getLocalChunk(this.msg.getFileID(), this.msg.getChunkNo());
            if (localChunk != null){
                localChunk.gotAnswer = true;
                localChunk.setData(this.msg.getBody());
            }
        }
    }

    public void getChunkHandler(){

        StoredChunk storedChunk = this.peer.state.getStoredChunk(this.msg);

        if (storedChunk == null){
            System.out.println("Peer_" + this.peer.getId() + "does not have requested chunk");
            return;
        }

        if (storedChunk.inProcess)
            return;

        storedChunk.inProcess = true;
        storedChunk.gotAnswer = false;


        //Wait for between 0-400ms
        try {
            Thread.sleep(new Random().nextInt(400));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        if (!storedChunk.gotAnswer){
            String header[] = {peer.getProtocolVersion(), "CHUNK", String.valueOf(peer.getId()), 
            storedChunk.getFileID(), String.valueOf(storedChunk.getChunkNo())};
    
            Message msg = new Message(header, storedChunk.getData(), -1);
            this.peer.sslHandler.sendToAllPeers(msg);
        }
        else {
            System.out.println("Chunk already sent by another peer, disregarding");
        }
        storedChunk.setData(null);
        storedChunk.inProcess = false;
    }

    public void removedHandler(){
        StoredChunk storedChunk = this.peer.state.getStoredChunk(this.msg);
        if (storedChunk == null)
            return;

        storedChunk.decPerceivedRepDegree();
        if (storedChunk.getPerceivedRepDegree() < storedChunk.getReplicationDegree()){
            storedChunk.gotAnswer = false;

            try {
                Thread.sleep(new Random().nextInt(400));
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            if (!storedChunk.gotAnswer) {
                byte[] chunkBody = this.peer.state.getStoredChunkBody(storedChunk);
                this.peer.threadPool.execute(new BackupInitiator(this.peer, storedChunk, chunkBody));
            }
        }
    }

    public void deleteHandler(){
        String fileID = this.msg.getFileID();

        Collection<StoredChunk> chunks = this.peer.state.getStoredChunks().values();

        for(StoredChunk chunk : chunks){
            System.out.println(chunk.getFileID());
            if(chunk.getFileID().equals(fileID)){
                this.peer.state.deleteStoredChunk(chunk);
            }
        }

        String pathString = this.peer.state.getPeerFileSystemFolder()+ "/" + fileID;
        Path path = Paths.get(pathString);
        try {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        } catch (Exception e){
            System.out.println("Failed to delete file/chunks, reason: " + e.getMessage());
        }
        this.peer.state.save();
    }

    public void putChunkHandler(){
        StoredChunk chunk = new StoredChunk(this.msg);

        // PutChunk of own file chunk
        if (this.peer.state.hasLocalFile(this.msg.getFileID()))
            return;

        if (!this.peer.state.hasStoredChunk(chunk)){
            chunk.gotAnswer = true;
            if (this.peer.state.getRemainingSpace() > chunk.size()) {
                if (chunk.getPerceivedRepDegree() < chunk.getReplicationDegree()) {
                    this.peer.state.saveStoredChunk(chunk);
                     try {
                        Thread.sleep(new Random().nextInt(400));
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    chunk.incPerceivedRepDegree();
                    this.sendStoredMessage(chunk);
                    this.peer.state.save();
                }
            }
        }
    }

    public void aliveHandler() throws UnknownHostException {
        String[] header = new String[]{this.peer.getProtocolVersion(), "OK", String.valueOf(this.peer.getId())};
        Message msg = new Message(header, -1);
        InetSocketAddress add = new InetSocketAddress(InetAddress.getByName(this.senderAddress.getHostName()), this.msg.getPort());

        SSLHandler.send(msg, add);
    }

    public boolean removePeerHandler(){
        return this.peer.sslHandler.removePeer(this.msg.getPort());
    }

    public void sendStoredMessage(StoredChunk chunk){
        String header[] = {peer.getProtocolVersion(), "STORED", String.valueOf(peer.getId()), chunk.getFileID()};
        Message msg = new Message(header, null, -1);
        peer.sslHandler.sendToAllPeers(msg);
    }

}
