package distributedbackup.filesystem;

import java.io.Serializable;

public class LocalChunk extends Chunk implements Serializable {

    private boolean restored;

    public LocalChunk(String fileID, int chunkNo, int repDegree){
        super(fileID, chunkNo, repDegree);
        this.data = null;
        this.restored = false;
    }

    public void setRestoredStatus(boolean status){
        this.restored = status;
    }

    public boolean isRestored(){
        return this.restored;
    }

    @Override
    public String toString() {
        return "Chunk" +
                " chunkNo=" + this.getChunkNo() +
                " PerceivedReplicationDegree=" + this.getPerceivedRepDegree(); //Change to perceived when done
    }
}
