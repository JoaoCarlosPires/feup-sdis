package distributedbackup.protocol;

import distributedbackup.service.Peer;

public class DeleteInitiator implements Runnable {

    private final Peer peer;
    private final String fileId;

    public DeleteInitiator(Peer peer, String fileId) {
        this.peer = peer;
        this.fileId = fileId;
    }

    @Override
    public void run() {

        String header[] = {peer.getProtocolVersion(), "DELETE", String.valueOf(peer.getId()), 
        this.fileId};

        Message msg = new Message(header, null, -1);

        this.peer.sslHandler.sendToAllPeers(msg);
    }
}
