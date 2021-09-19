package distributedbackup.protocol;

import distributedbackup.filesystem.Chunk;
import distributedbackup.service.Peer;

public class BackupInitiator implements Runnable {

    private final Peer peer;
    private final Message msg;
    private final Chunk chunk;

    public BackupInitiator(Peer peer, Chunk chunk, byte[] body){
        this.peer = peer;
        this.chunk = chunk;

        String header[] = {peer.getProtocolVersion(), "PUTCHUNK", String.valueOf(peer.getId()), chunk.getFileID(), String.valueOf(chunk.getChunkNo()), String.valueOf(chunk.getReplicationDegree())};

        this.msg = new Message(header, body, -1);
    }

    @Override
    public void run(){

        int delay = 500, attempts = 0;

        while (this.chunk.getPerceivedRepDegree() < this.chunk.getReplicationDegree()) {
            delay *= 2;

            if (attempts >= 5)
                break;

            peer.sslHandler.sendToAllPeers(this.msg);
            try {
                Thread.sleep(delay);
            }catch(Exception e){
                System.out.println(e.getMessage());
            }

            ++attempts;
        }
    }

}
