package distributedbackup.filesystem;

import distributedbackup.protocol.Message;
import distributedbackup.service.Peer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class State implements Serializable {
    private final int peerID;
    private final String peerFileSystemFolder;
    private long availableSpace;
    private long usedSpace;

    private final ConcurrentHashMap<String, LocalFileInfo> localFiles;
    private final ConcurrentHashMap<String, StoredChunk> storedChunks;


    public State(int peerID){
        this.peerID = peerID;
        this.peerFileSystemFolder = "data/peer_" + this.peerID;
        this.availableSpace = 2000000000;
        this.usedSpace = 0;
        this.localFiles = new ConcurrentHashMap<>();
        this.storedChunks = new ConcurrentHashMap<>();
        System.out.println("New internal state has been generated");
        this.save();
    }


    public synchronized void save(){
        Path directory = Paths.get(this.peerFileSystemFolder);
        try {
            Files.createDirectories(directory);

            new File(getDBPath(this.peerID)).createNewFile();
        } catch (Exception e) {
            System.out.println("Failed to create database file, reason: " + e.getMessage());
            e.printStackTrace();
        }

        try {
            FileOutputStream fileOut = new FileOutputStream(getDBPath(this.peerID));
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this);
            out.flush();
            out.close();
            fileOut.close();
        } catch (Exception e) {
            System.out.println("Failed to create database file, reason: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static synchronized State load(int peerID){
        State existingState = null;
        try {
            FileInputStream fileIn = new FileInputStream(getDBPath(peerID));
            ObjectInputStream in = new ObjectInputStream(fileIn);
            existingState = (State) in.readObject();
            in.close();
            fileIn.close();
        } catch (Exception e) {
            System.out.println("Unable to load database, reason: " + e.getMessage());
            e.printStackTrace();
        }

        if (existingState == null)
            return new State(peerID);

        System.out.println("Previous state has been loaded successfully");
        return existingState;
    }

    /* LOCAL FILES */

    public void saveLocalFile(LocalFile file){
        LocalFileInfo fileInfo = new LocalFileInfo(file);

        this.localFiles.put(file.getFileID(), fileInfo);
        this.save();
    }

    public boolean hasLocalFile(String fileID){
        return this.localFiles.containsKey(fileID);
    }


    public LocalFileInfo getLocalFileByPathname(String pathname){
        for (LocalFileInfo file : this.localFiles.values())
            if (file.getFilepath().equals(pathname))
                return file;

        return null;
    }

    public void printLocalFiles(){
        StringBuilder text = new StringBuilder();

        for (LocalFileInfo file : localFiles.values())
            text.append(file.toString()).append('\n');

        System.out.println(text);
    }

    /* STORED CHUNKS */

    public ConcurrentHashMap<String, StoredChunk> getStoredChunks(){
            return this.storedChunks;
    }

    public LocalChunk getLocalChunk(String fileID, int chunkNo){
        LocalFileInfo file = this.localFiles.get(fileID);
        if (file == null)
            return null;

        if (chunkNo >= file.getChunkCount())
            return null;

        return file.getChunk(chunkNo);
    }

    public LocalChunk getLocalChunk(Chunk chunk){
        LocalFileInfo file = this.localFiles.get(chunk.getFileID());
        if (file == null)
            return null;

        if (chunk.getChunkNo() >= file.getChunkCount())
            return null;

        return file.getChunk(chunk.getChunkNo());
    }


    public void saveStoredChunk(StoredChunk chunk){

        String chunkPath = generateChunkFilepath(chunk);
        Path path = Paths.get(chunkPath);
        try {
            Files.createDirectories(path.getParent());
            FileOutputStream out = new FileOutputStream(chunkPath);
            out.write(chunk.getData());
            out.close();
        }catch(Exception e){
            System.out.println("Failed to save chunk (" + chunkPath + "), reason: " + e.getMessage());
        }
        chunk.setData(new byte[0]); //Remove chunk data from memory
        String chunkKey = generateChunkKey(chunk);
        this.storedChunks.put(chunkKey, chunk);
        this.usedSpace += chunk.size();
        System.out.println("Successfully saved chunk (" + chunkPath + ")");
        this.save();
    }

    public boolean hasStoredChunk(Chunk chunk){
        return this.storedChunks.containsKey(generateChunkKey(chunk));
    }

    public synchronized StoredChunk getStoredChunk(Message msg){
        StoredChunk storedChunk = this.storedChunks.get(generateChunkKey(msg));
        if (storedChunk == null)
            return null;

        storedChunk.setData(getStoredChunkBody(storedChunk));
        return storedChunk;
    }

    public synchronized void incPerceivedRepDegree(Chunk chunk){
        StoredChunk storedChunk = this.storedChunks.get(generateChunkKey(chunk));
        if (storedChunk == null)
            return;

        storedChunk.incPerceivedRepDegree();
    }

    public synchronized void decPerceivedRepDegree(Chunk chunk){
        StoredChunk storedChunk = this.storedChunks.get(generateChunkKey(chunk));
        if (storedChunk == null)
            return;

        storedChunk.decPerceivedRepDegree();
    }

    public void removeLocalFile(LocalFileInfo file){
        this.localFiles.remove(file.getFileID());
    }

    public byte[] getStoredChunkBody(StoredChunk chunk){
        Path filepath = Paths.get(generateChunkFilepath(chunk));
        byte[] body;
        try {
            body = Files.readAllBytes(filepath);
        }catch (Exception e){
            System.out.println("Failed to read stored chunk, reason: " + e.getMessage());
            return null;
        }
        return body;
    }

    public void printStoredChunks(){
        StringBuilder text = new StringBuilder();

        for (StoredChunk chunk : storedChunks.values())
            text.append(chunk.toString()).append('\n');

        System.out.println(text);
    }

    public void deleteStoredChunk(StoredChunk chunk){
        String key = this.generateChunkKey(chunk);
        this.storedChunks.remove(key);
    }

    public StoredChunk getHighestRepStoredChunk(){

        StoredChunk repChunk = null;
        int highestRep = -1;

        for (StoredChunk chunk : this.storedChunks.values()){
            if (chunk.getPerceivedRepDegree() > highestRep) {
                repChunk = chunk;
                highestRep = chunk.getPerceivedRepDegree();
            }
        }
        return repChunk;
    }

    /* OTHERS */


    public void reclaim(int kBytes, Peer peer){
        System.out.println("Reclaiming space...");
        this.availableSpace = kBytes * 1000;
        StoredChunk delChunk = null;
        while(this.getRemainingSpace() < 0){
            delChunk = this.getHighestRepStoredChunk();
            if (delChunk == null)
                return;
            String chunkPath = this.generateChunkFilepath(delChunk);
            Path path = Paths.get(chunkPath);

            try{
                Files.delete(path);
                System.out.println("Deleted chunk " + chunkPath + "with perRepDegree=" + delChunk.getPerceivedRepDegree());
            }catch (Exception e){
                System.out.println("Failed to delete chunk, reason: " + e.getMessage());
            }

            String header[] = {peer.getProtocolVersion(), "REMOVED", String.valueOf(peer.getId()), 
            delChunk.getFileID(), String.valueOf(delChunk.getChunkNo())};

            Message msg = new Message(header, null, -1);
            peer.sslHandler.sendToAllPeers(msg);

            this.usedSpace -= delChunk.size();
            this.deleteStoredChunk(delChunk);
        }
    }

    public String getPeerFileSystemFolder() {
        return peerFileSystemFolder;
    }


    public long getRemainingSpace(){
        return this.availableSpace - this.usedSpace;
    }

    public static boolean hasExistingDatabase(int peerID){
        File f = new File(getDBPath(peerID));
        return f.exists() && !f.isDirectory();
    }

    private String generateChunkFilepath(Chunk chunk){
        return this.peerFileSystemFolder + "/" + chunk.getFileID() + "/" + chunk.getChunkNo();
    }

    private String generateChunkKey(Chunk chunk){
        return chunk.getFileID() + "_" + chunk.getChunkNo();
    }
    private String generateChunkKey(Message msg){
        return msg.getFileID() + "_" + msg.getChunkNo();
    }

    private static String getDBPath(int peerID){
        return "data/peer_" + peerID + "/db";
    }

    public void printSpace(){
        String text = "used Space: " + this.usedSpace/1000 + "KBs/" + this.availableSpace/1000 + "KBs";
        System.out.println(text);
    }
}
