import utils.*;

import java.util.*;
import java.net.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static utils.MessageUtils.byteArrayToInt;

class MessageProcessor implements Runnable {

    private final byte[] data;

    public MessageProcessor(byte[] data) {
        this.data = data;
    }

    private void processMessage(byte[] data) throws IOException, InterruptedException {
        ArrayList<byte[]> parsedMessage = MessageUtils.parseMessage(data);
        byte[] version = parsedMessage.get(0);
        byte[] msgType = parsedMessage.get(1);
        byte[] senderID = parsedMessage.get(2);
        byte[] fileID = parsedMessage.get(3);
        byte[] chunkNo;
        String fileKey = new String(fileID);
        Random rand = new Random();

        if (Arrays.equals(senderID, (Peer.peerInfo.peerID+"").getBytes(StandardCharsets.ISO_8859_1))) {
            return;
        }
        if (Arrays.equals(msgType, "PUTCHUNK".getBytes(StandardCharsets.ISO_8859_1))) {
            //<Version> PUTCHUNK <SenderId> <FileId> <ChunkNo> <ReplicationDeg> <Body>
            //    0        1          2        3         4            5           6
            chunkNo = parsedMessage.get(4);
            byte[] repDegree = parsedMessage.get(5);
            byte[] body = parsedMessage.get(6);
            ChunkKey chunkKey = new ChunkKey(fileKey, byteArrayToInt(chunkNo));

            if (Peer.peerInfo.chunksStored.get(chunkKey) != null) {
                Peer.peerInfo.chunksStored.get(chunkKey).setReclaimed(false);
                Thread.sleep(rand.nextInt(350) + 50);
                byte[] header = MessageUtils.createHeader(MessageUtils.protocolVersion1,MessageUtils.MessageType.STORED,Peer.peerInfo.peerID,fileID, byteArrayToInt(chunkNo),-1);

                MulticastSender.send(header, Peer.controlMultCast);
                return;
            }

            if (Peer.peerInfo.filesStored.get(fileKey) != null)
                return;

            if (Peer.peerInfo.getBackupSpace() != -1 && Peer.peerInfo.getFreeSpace() < body.length) {
                Set<ChunkKey> keys = Peer.peerInfo.chunksStored.keySet();
                ArrayList<ChunkPair> chunks = new ArrayList<>();
                for (ChunkKey key : keys) {
                    chunks.add(new ChunkPair(key,Peer.peerInfo.chunksStored.get(key).getDifferenceDegree()));
                }
                Collections.sort(chunks);
                Collections.reverse(chunks);
                int freeableSpace = 0;
                for (ChunkPair chunkPair : chunks) {
                    if (chunkPair.getValue() > 0)
                        freeableSpace += Peer.peerInfo.chunksStored.get(chunkPair.getKey()).getSize();
                    else break;
                }

                if (freeableSpace >= body.length) {
                    while (Peer.peerInfo.getFreeSpace() < body.length) {
                        ChunkKey chunkKeyToRemove = chunks.get(0).getKey();
                        String fileKeyToRemove = chunkKeyToRemove.getFileKey();
                        int chunkNoToRemove = chunkKeyToRemove.getNumber();
                        Peer.peerInfo.chunksStored.remove(chunkKeyToRemove);

                        FileUtils.deleteFile("../peers/peer" + Peer.peerInfo.peerID + "/chunks/" + fileKeyToRemove + chunkNoToRemove);
                        System.out.println("Deleted chunk " + chunkNoToRemove + " of file " + fileKeyToRemove + " in the system");

                        byte[] header = MessageUtils.createHeader(MessageUtils.protocolVersion1, MessageUtils.MessageType.REMOVED, Peer.peerInfo.peerID, fileKeyToRemove.getBytes(), chunkNoToRemove, -1);
                        MulticastSender.send(header, Peer.controlMultCast);
                    }
                }
                else
                    return;
            }

            ChunkInfo chunkInfo = new ChunkInfo(MessageUtils.byteArrayToInt(senderID), MessageUtils.byteArrayToInt(repDegree), body.length);
            Peer.peerInfo.chunksStored.put(chunkKey,chunkInfo);
            try {
                FileUtils.chunkToFile(body, "../peers/peer" + Peer.peerInfo.peerID + "/chunks/" + chunkKey.getFileKey() + chunkKey.getNumber());
            } catch (IOException e) {
                System.err.println("Error storing chunk");
                Peer.peerInfo.chunksStored.remove(chunkKey);
                return;
            }

            Thread.sleep(rand.nextInt(350) + 50);

            if (Peer.protocolVersion.equals(MessageUtils.protocolVersion1)) {
                chunkInfo.increaseRepDegree(Peer.peerInfo.peerID);
                byte[] header = MessageUtils.createHeader(MessageUtils.protocolVersion1,MessageUtils.MessageType.STORED,Peer.peerInfo.peerID,fileID,MessageUtils.byteArrayToInt(chunkNo),-1);

                MulticastSender.send(header, Peer.controlMultCast);
                return;
            }
            if (Peer.protocolVersion.equals(MessageUtils.protocolVersion2)) {
                if (chunkInfo.repDegreeNotSatisfied()) {
                    byte[] header = MessageUtils.createHeader(MessageUtils.protocolVersion1,MessageUtils.MessageType.STORED,Peer.peerInfo.peerID,fileID,MessageUtils.byteArrayToInt(chunkNo),-1);
                    MulticastSender.send(header, Peer.controlMultCast);
                    chunkInfo.increaseRepDegree(Peer.peerInfo.peerID);
                } else {
                    FileUtils.deleteFile("../peers/peer" + Peer.peerInfo.peerID + "/chunks/" + chunkKey.getFileKey() + chunkKey.getNumber());
                    Peer.peerInfo.chunksStored.remove(chunkKey);
                }
            }
            return;
        }
        if (Arrays.equals(msgType, "STORED".getBytes(StandardCharsets.ISO_8859_1))) {
            //<Version> STORED <SenderId> <FileId> <ChunkNo>
            //    0       1        2         3         4
            chunkNo = parsedMessage.get(4);
            ChunkKey chunkKey = new ChunkKey(fileKey, byteArrayToInt(chunkNo));
            FileInfo fileInfo = Peer.peerInfo.filesStored.get(fileKey);

            if (Peer.protocolVersion.equals(MessageUtils.protocolVersion2) && new String(version).equals(MessageUtils.protocolVersion2))
            {
                if (fileInfo == null) {
                    byte[] header = MessageUtils.createHeader(MessageUtils.protocolVersion1, MessageUtils.MessageType.DELETE, Peer.peerInfo.peerID, fileID, -1, -1);
                    MulticastSender.send(header, Peer.controlMultCast);
                }
                return;
            }

            ChunkInfo chunkInfo = Peer.peerInfo.chunksStored.get(chunkKey);
            if (chunkInfo != null) {
                chunkInfo.increaseRepDegree(byteArrayToInt(senderID));
            }

            if (fileInfo != null) {
                ArrayList<Integer> chunkPeers = fileInfo.chunks.get(byteArrayToInt(chunkNo));
                if (!chunkPeers.contains(byteArrayToInt(senderID))) {
                    System.out.println("Chunk " + byteArrayToInt(chunkNo) + " from file " + fileKey + " was stored in Peer" + byteArrayToInt(senderID));
                    chunkPeers.add(byteArrayToInt(senderID));
                }
            }
            return;
        }
        if (Arrays.equals(msgType, "GETCHUNK".getBytes(StandardCharsets.ISO_8859_1))) {
            //<Version> GETCHUNK <SenderId> <FileId> <ChunkNo>
            //    0        1         2         3         4
            chunkNo = parsedMessage.get(4);
            ChunkKey chunkKey = new ChunkKey(fileKey, byteArrayToInt(chunkNo));

            ChunkInfo chunkInfo = Peer.peerInfo.chunksStored.get(chunkKey);
            if (chunkInfo == null)
                return;

            chunkInfo.setRequested(true);
            Thread.sleep(rand.nextInt(400));
            if (chunkInfo.getRequested()) {
                chunkInfo.setRequested(false);
                byte[] header = MessageUtils.createHeader(MessageUtils.protocolVersion1,MessageUtils.MessageType.CHUNK,Peer.peerInfo.peerID,fileID,MessageUtils.byteArrayToInt(chunkNo),-1);
                byte[] chunkData = FileUtils.fileToChunk("../peers/peer" + Peer.peerInfo.peerID + "/chunks/" + chunkKey.getFileKey() + chunkKey.getNumber());
                byte[] message = MessageUtils.addBody(header,chunkData);
                MulticastSender.send(message,Peer.restoreMultCast);
            }
            return;
        }
        if (Arrays.equals(msgType, "CHUNK".getBytes(StandardCharsets.ISO_8859_1))) {
            //<Version> CHUNK <SenderId> <FileId> <ChunkNo> <Body>
            //    0       1        2         3        4       5
            chunkNo = parsedMessage.get(4);
            ChunkKey chunkKey = new ChunkKey(fileKey, byteArrayToInt(chunkNo));

            ChunkInfo chunkInfo = Peer.peerInfo.chunksStored.get(chunkKey);
            if (chunkInfo == null)
                return;
            chunkInfo.setRequested(false);
            return;
        }
        if (Arrays.equals(msgType, "DELETE".getBytes(StandardCharsets.ISO_8859_1))) {
            //<Version> DELETE <SenderId> <FileId>
            //    0       1        2         3
            Iterator<ChunkKey> iterator = Peer.peerInfo.chunksStored.keySet().iterator();

            while (iterator.hasNext()) {
                ChunkKey chunkKey = iterator.next();
                if (chunkKey.getFileKey().equals(fileKey) && Peer.peerInfo.chunksStored.get(chunkKey).getInitiatorID() == MessageUtils.byteArrayToInt(senderID)) {
                    iterator.remove();
                    FileUtils.deleteFile("../peers/peer" + Peer.peerInfo.peerID + "/chunks/" + chunkKey.getFileKey() + chunkKey.getNumber());
                }
            }
            return;
        }
        if (Arrays.equals(msgType, "REMOVED".getBytes(StandardCharsets.ISO_8859_1))) {
            //<Version> REMOVED <SenderId> <FileId> <ChunkNo>
            //    0       1        2         3          4
            chunkNo = parsedMessage.get(4);
            ChunkKey chunkKey = new ChunkKey(fileKey, byteArrayToInt(chunkNo));

            ChunkInfo chunkInfo = Peer.peerInfo.chunksStored.get(chunkKey);
            if (chunkInfo != null) {
                chunkInfo.decreaseRepDegree(byteArrayToInt(senderID));
                if (chunkInfo.repDegreeNotSatisfied()) {
                    chunkInfo.setReclaimed(true);
                    Thread.sleep(rand.nextInt(350) + 50);
                    if (chunkInfo.getReclaimed()) {
                        chunkInfo.setReclaimed(false);
                        int timeoutSec = 1;

                        while (timeoutSec <= 16) {
                            if (Peer.peerInfo.chunksStored.get(chunkKey).repDegreeNotSatisfied()) {
                                byte[] header = MessageUtils.createHeader(MessageUtils.protocolVersion1,MessageUtils.MessageType.PUTCHUNK,Peer.peerInfo.peerID,fileID,MessageUtils.byteArrayToInt(chunkNo),chunkInfo.getDesiredRepDegree());
                                byte[] chunkData = FileUtils.fileToChunk("../peers/peer" + Peer.peerInfo.peerID + "/chunks/" + chunkKey.getFileKey() + chunkKey.getNumber());
                                byte[] message = MessageUtils.addBody(header,chunkData);
                                MulticastSender.send(message,Peer.backupMultCast);
                                try {
                                    Thread.sleep(100);
                                } catch (InterruptedException ignored) {
                                }
                                header = MessageUtils.createHeader(MessageUtils.protocolVersion1,MessageUtils.MessageType.STORED,Peer.peerInfo.peerID,fileID, byteArrayToInt(chunkNo),-1);
                                MulticastSender.send(header, Peer.controlMultCast);
                            } else break;

                            try {
                                Thread.sleep(timeoutSec* 1000L);
                            } catch (InterruptedException ignored) {
                            }
                            timeoutSec *= 2;
                        }
                    }
                }
            }
            FileInfo fileInfo = Peer.peerInfo.filesStored.get(fileKey);
            if (fileInfo != null) {
                fileInfo.chunks.get(byteArrayToInt(chunkNo)).remove(Integer.valueOf(byteArrayToInt(senderID)));
            }
        }
    }

    @Override
    public void run() {
        try {
            processMessage(data);
            Peer.saveState();
        } catch (InterruptedException | IOException ignored) {
        }
    }
}

public class MulticastReceiver implements Runnable {
    protected String address;
    protected int port;

    public MulticastReceiver(String[] multicast) {
        this.address = multicast[0];
        this.port = Integer.parseInt(multicast[1]);
    }

    public static byte[] receive(String[] multicast, int timeoutMs) throws IOException {
        MulticastSocket socket = new MulticastSocket(Integer.parseInt(multicast[1]));
        InetAddress group = InetAddress.getByName(multicast[0]);
        socket.joinGroup(group);
        byte[] buffer = new byte[FileUtils.chunkSize + MessageUtils.maxHeaderSize];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        socket.setSoTimeout(timeoutMs);
        socket.receive(packet);
        byte[] data = Arrays.copyOf(packet.getData(),packet.getLength());
        //System.out.println("Received packet with length " + packet.getLength() +  ".");
        socket.leaveGroup(group);
        socket.close();
        return data;
    }

    public void run() {
        try {
            MulticastSocket socket = new MulticastSocket(port);
            InetAddress group = InetAddress.getByName(address);
            socket.joinGroup(group);
            while (true) {
                byte[] buffer = new byte[FileUtils.chunkSize + MessageUtils.maxHeaderSize];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                socket.receive(packet);
                byte[] data = Arrays.copyOf(packet.getData(),packet.getLength());
                //System.out.println("Received packet with length " + packet.getLength() + ".");
                new Thread(new MessageProcessor(data)).start();
            }
        } catch (IOException e) {
            System.err.println("Invalid address or port for a multicast channel");
        }
    }

}
