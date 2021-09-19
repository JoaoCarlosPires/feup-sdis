package distributedbackup.filesystem;


import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.ArrayList;

public class LocalFileInfo implements Serializable {

    protected ArrayList<LocalChunk> chunks;
    protected String filepath;
    protected String fileID;
    protected int repDegree;
    protected int peerID;

    public LocalFileInfo(String filepath, int repDegree, int peerID) {
        this.filepath = filepath;
        this.repDegree = repDegree;
        this.peerID = peerID;
        this.fileID = hash(this.filepath);
        this.chunks = new ArrayList<>();
    }

    public LocalFileInfo(LocalFile file) {
        this.filepath = file.getFilepath();
        this.repDegree = file.getRepDegree();
        this.peerID = file.getPeer().getId();
        this.fileID = file.getFileID();
        this.chunks = file.getChunks();
    }


    public String getFileID() {
        return fileID;
    }

    public int getRepDegree() {
        return repDegree;
    }


    public String getFilepath() {
        return filepath;
    }

    public LocalChunk getChunk(int chunkNo) {
        return this.chunks.get(chunkNo);
    }

    public ArrayList<LocalChunk> getChunks() {
        return chunks;
    }

    public static String hash(String pathname) {
        MessageDigest digest;
        String hashInfo = pathname;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            Path file = Paths.get(pathname);
            BasicFileAttributes attr =
                    Files.readAttributes(file, BasicFileAttributes.class);
            hashInfo += attr.lastModifiedTime();
        } catch (Exception e) {
            System.out.println("Hash algorithm not found: " + e.getMessage());
            return null;
        }


        byte[] hash = digest.digest(hashInfo.getBytes(StandardCharsets.UTF_8));
        return String.format("%064x", new BigInteger(1, hash));
    }

    public void clearChunks(){
        this.chunks.clear();
    }

    public int getChunkCount(){
        return this.chunks.size();
    }

    @Override
    public String toString() {

        StringBuilder text = new StringBuilder( );
        text.append("Local File - ");
        text.append(" Filepath=");
        text.append(this.getFilepath());
        text.append(" Backup peer ID=");
        text.append(this.peerID);
        text.append(" DesiredReplicationDegree=");
        text.append(this.getRepDegree());
        text.append('\n');

        for (LocalChunk chunk: this.chunks) {
            text.append(chunk.toString());
            text.append('\n');
        }

        return text.toString();
    }
}