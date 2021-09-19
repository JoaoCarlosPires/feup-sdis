package distributedbackup.protocol;

import distributedbackup.filesystem.LocalChunk;
import distributedbackup.service.Peer;

public class RestoreInitiator implements Runnable {

    private LocalChunk chunk;
    private Peer peer;

    public RestoreInitiator(Peer peer, LocalChunk chunk){
        this.chunk = chunk;
        this.peer = peer;
    }

    @Override
    public void run(){

        String header[] = {peer.getProtocolVersion(), "GETCHUNK", String.valueOf(peer.getId()), 
                        chunk.getFileID(), String.valueOf(chunk.getChunkNo())};


        Message msg = new Message(header, null, -1);

        this.peer.sslHandler.sendToAllPeers(msg);

        int attempts = 0;
        while (chunk.getData() == null && attempts < 20){
            ++attempts;
            try { Thread.sleep(50); } catch (InterruptedException e) {}
        }
    }
}
