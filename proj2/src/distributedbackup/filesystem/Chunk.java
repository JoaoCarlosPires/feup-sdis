package distributedbackup.filesystem;

import distributedbackup.protocol.Message;

import java.io.Serializable;

public class Chunk implements Serializable {
    protected String fileID;
    protected int chunkNo;
    protected int repDegree;
    protected int perceivedRepDegree;
    protected byte[] data;
    public boolean gotAnswer;
    public boolean inProcess;

    public static int MAX_SIZE = 64000;

    public Chunk(String fileID, int chunkNo, int repDegree){
        this.fileID = fileID;
        this.chunkNo = chunkNo;
        this.repDegree = Math.min(repDegree, 9);
        this.perceivedRepDegree = 0;
        this.gotAnswer = false;
        this.inProcess = false;
    }

    public Chunk(Message msg){
        this.fileID = msg.getFileID();
        this.chunkNo = msg.getChunkNo();
        this.repDegree = msg.getReplicationDegree();
        this.perceivedRepDegree = 0;
        this.gotAnswer = false;
        this.inProcess = false;
    }


    public int getChunkNo() {
        return chunkNo;
    }

    public String getFileID() {
        return fileID;
    }

    public int getReplicationDegree() { return repDegree;}

    public synchronized void incPerceivedRepDegree(){
        ++this.perceivedRepDegree;
    }

    public synchronized void decPerceivedRepDegree(){
        --this.perceivedRepDegree;
    }

    public int getPerceivedRepDegree() {
        return perceivedRepDegree;
    }

    public void setData(byte[] data){
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }
}
