package distributedbackup.filesystem;

import distributedbackup.protocol.Message;

import java.io.Serializable;

public class StoredChunk extends Chunk implements Serializable {


    private int size;

    public StoredChunk(Message msg){
        super(msg.getFileID(), msg.getChunkNo(), msg.getReplicationDegree());
        this.data = msg.getBody();
        this.size = this.data.length;
    }

    public StoredChunk(StoredChunk chunk, byte[] body){
        super(chunk.getFileID(), chunk.getChunkNo(), chunk.getReplicationDegree());
        this.data = body;
        this.size = this.data.length;
    }

    public byte[] getData() {
        return data;
    }

    public int size(){
        return this.size;
    }


    @Override
    public String toString() {

        return "Chunk" +
                " chunkNo=" + this.getChunkNo() +
                " size=" + this.size / 1000 + "KBs" +
                " replicationDegree=" + this.getReplicationDegree() +
                " perceivedReplicationDegree=" + this.getPerceivedRepDegree();
    }

}




