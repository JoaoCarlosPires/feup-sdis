package distributedbackup.protocol;

import distributedbackup.service.Tracker;

import java.net.InetSocketAddress;

public class PeerCheck implements Runnable {

    private Tracker tracker;
    private String[] header;
    private long timeout = 2000;

    public PeerCheck(Tracker tracker) {
        this.tracker = tracker;
        this.header = new String[]{tracker.getProtocolVersion(), "ALIVE", String.valueOf(tracker.getId())};
    }

    @Override
    public void run() {
        for (InetSocketAddress peer : tracker.sslHandler.getPeers().values()) {
            Message msg = new Message(header, this.tracker.getPort());
            tracker.sslHandler.send(msg, peer);
        }

        long currentTime = System.currentTimeMillis();
        for (Integer id : tracker.sslHandler.getTimestamps().keySet()) {
            if (!this.tracker.sslHandler.getPeers().containsKey(id))
                continue;
            long timestamp = tracker.sslHandler.getTimestamps().get(id);
            if (currentTime - timestamp > timeout) {

                tracker.sslHandler.removePeer(id);

                String[] remHeader = new String[]{tracker.getProtocolVersion(), "REMPEER", String.valueOf(tracker.getId())};

                Message msg = new Message(remHeader, null, id);
                tracker.sslHandler.sendToAllPeers(msg);
                tracker.sslHandler.sendToAllTrackers(msg);
            }
        }
    }
}
