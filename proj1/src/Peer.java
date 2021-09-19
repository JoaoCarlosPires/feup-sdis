import utils.*;

import java.io.*;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

import java.security.NoSuchAlgorithmException;

public class Peer implements PeerInterface {
    public static PeerInfo peerInfo;
    public static String protocolVersion;
    public static String[] controlMultCast = new String[2];
    public static String[] backupMultCast = new String[2];
    public static String[] restoreMultCast = new String[2];

    @Override
    public byte[] backup(String fileName, int repDegree) throws RemoteException {
        System.out.println("Backup: " + fileName + " " + repDegree);

        byte[] fileID;
        String fileKey;
        List<byte[]> chunksData;

        try {
            fileID = FileUtils.getFileID(fileName);
            fileKey = new String(fileID);

            if (peerInfo.filesStored.get(fileKey) != null) {
                return "File Already Backed Up".getBytes();
            }

            chunksData = FileUtils.fileToChunks("../" + fileName);
        } catch (IOException e) {
            return "File Not Found".getBytes();
        } catch (NoSuchAlgorithmException e) {
            return "Failed to Encrypt Filename".getBytes();
        }

        try {
            peerInfo.filesStored.put(fileKey,new FileInfo(FileUtils.getFileName(fileName),chunksData.size(), repDegree));

            boolean repDegreeNotSatisfied = true;
            int timeoutSec = 1;

            while (repDegreeNotSatisfied && timeoutSec <= 16) {
                for (int i = 0; i < chunksData.size(); i++) {
                    if (peerInfo.filesStored.get(fileKey).repDegreeNotSatisfied(i)) {
                        byte[] header = MessageUtils.createHeader(MessageUtils.protocolVersion1,MessageUtils.MessageType.PUTCHUNK,peerInfo.peerID,fileID,i,repDegree);
                        byte[] message = MessageUtils.addBody(header,chunksData.get(i));
                        MulticastSender.send(message, backupMultCast);
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException ignored) {
                        }
                    }
                }
                try {
                    Thread.sleep(timeoutSec* 1000L);
                } catch (InterruptedException ignored) {
                }
                timeoutSec *= 2;
                repDegreeNotSatisfied = false;
                for (int i = 0; i < chunksData.size(); i++) {
                    if (peerInfo.filesStored.get(fileKey).repDegreeNotSatisfied(i)) {
                        repDegreeNotSatisfied = true;
                        break;
                    }
                }
            }

            if (peerInfo.filesStored.get(fileKey).notStored()) {
                try {
                    byte[] header = MessageUtils.createHeader(MessageUtils.protocolVersion1, MessageUtils.MessageType.DELETE, peerInfo.peerID, fileKey.getBytes(StandardCharsets.US_ASCII), -1, -1);
                    MulticastSender.send(header, controlMultCast);
                } catch (IOException e) {
                    return "Failed to Communicate with Other Peers".getBytes();
                }
                peerInfo.filesStored.remove(fileKey);
                saveState();
                return "Failed to Store All the Chunks of the File, Backup Canceled".getBytes();
            }

            if (repDegreeNotSatisfied) {
                saveState();
                return "File Stored with Less than Desired Replication Degree".getBytes();
            }

        } catch (IOException e) {
            return "Failed to Communicate with Other Peers".getBytes();
        }
        System.out.println("Stored file " + fileKey + " in the system");
        saveState();
        return "File Backed Up".getBytes();
    }

    @Override
    public byte[] restore(String fileName) throws RemoteException {
        System.out.println("Restore: " + fileName);
        Set<String> keys = peerInfo.filesStored.keySet();
        String fileKey = "";
        for (String k : keys) {
            if (peerInfo.filesStored.get(k).getFileName().equals(fileName)) {
                fileKey = k;
                break;
            }

        }
        if (fileKey.isEmpty())
            return "File is Not Backed Up".getBytes();

        List<byte[]> chunksData = new ArrayList<>();
        try {
            for (int i = 0; i < peerInfo.filesStored.get(fileKey).chunks.size(); i++) {
                byte[] header = MessageUtils.createHeader(MessageUtils.protocolVersion1, MessageUtils.MessageType.GETCHUNK, peerInfo.peerID, fileKey.getBytes(StandardCharsets.US_ASCII), i, -1);
                MulticastSender.send(header, controlMultCast);
                byte[] msg = MulticastReceiver.receive(restoreMultCast, 2000);
                ArrayList<byte[]> parsedMessage = MessageUtils.parseMessage(msg);
                //<Version> CHUNK <SenderId> <FileId> <ChunkNo> <Body>
                //    0       1        2         3        4       5
                boolean sameMsgType = Arrays.equals(parsedMessage.get(1), "CHUNK".getBytes(StandardCharsets.ISO_8859_1));
                boolean sameFile = new String(parsedMessage.get(3)).equals(fileKey);
                boolean sameChunk = new String(parsedMessage.get(4)).equals(String.valueOf(i));

                while (!(sameMsgType && sameFile && sameChunk)) {
                    MulticastSender.send(header, controlMultCast);
                    msg = MulticastReceiver.receive(restoreMultCast, 1000);
                    parsedMessage = MessageUtils.parseMessage(msg);
                    sameMsgType = Arrays.equals(parsedMessage.get(1), "CHUNK".getBytes(StandardCharsets.ISO_8859_1));
                    sameFile = new String(parsedMessage.get(3)).equals(fileKey);
                    sameChunk = new String(parsedMessage.get(4)).equals(String.valueOf(i));
                }
                chunksData.add(parsedMessage.get(5));
            }
        } catch (IOException e) {
            if (e.getClass().equals(SocketTimeoutException.class)) {
                return "Timeout Reached".getBytes();
            } else {
                return "Failed to Communicate with Other Peers".getBytes();
            }
        }

        try {
            FileUtils.chunksToFile(chunksData, "../peers/peer" + Peer.peerInfo.peerID + "/restore/" + fileName);
        } catch (IOException e) {
            return "Failed to Create Restored File".getBytes();
        }

        return "File Restored".getBytes();
    }

    @Override
    public byte[] delete(String fileName) throws RemoteException {
        System.out.println("Delete: " + fileName);
        Set<String> keys = peerInfo.filesStored.keySet();
        boolean fileExisted = false;
        for (String fileKey : keys) {
            if (peerInfo.filesStored.get(fileKey).getFileName().equals(fileName)) {
                fileExisted = true;
                try {
                    byte[] header = MessageUtils.createHeader(MessageUtils.protocolVersion1, MessageUtils.MessageType.DELETE, peerInfo.peerID, fileKey.getBytes(StandardCharsets.US_ASCII), -1, -1);
                    MulticastSender.send(header, controlMultCast);
                } catch (IOException e) {
                    return "Failed to Communicate with Other Peers".getBytes();
                }
                peerInfo.filesStored.remove(fileKey);
            }
        }
        if (fileExisted) {
            saveState();
            return "File Deleted".getBytes();
        }
        else return "File is Not Backed Up".getBytes();
    }

    @Override
    public byte[] reclaim(int diskSpace) throws RemoteException {
        System.out.println("Reclaim: " + diskSpace);
        diskSpace*=1000; // KB -> B
        if (peerInfo.getBackupSpace() != -1 && diskSpace >= peerInfo.getBackupSpace()) {
            peerInfo.setBackupSpace(diskSpace);
            saveState();
            return "Disk Space Updated".getBytes();
        }
        peerInfo.setBackupSpace(diskSpace);
        /* If the new backup space is lower than the previous one or if the new one is not enough to keep the chunks stored */
        if (peerInfo.getBackupSpace() < peerInfo.getOccupiedSpace()) {
            Set<ChunkKey> keys = peerInfo.chunksStored.keySet();
            ArrayList<ChunkPair> chunks = new ArrayList<>();
            for (ChunkKey key : keys) {
                chunks.add(new ChunkPair(key,peerInfo.chunksStored.get(key).getDifferenceDegree()));
            }

            Collections.sort(chunks);
            Collections.reverse(chunks);

            int spaceToFree = -peerInfo.getFreeSpace();
            for (ChunkPair chunkPair : chunks) {
                if (spaceToFree <= 0) {
                    break;
                }

                String fileKey = chunkPair.getKey().getFileKey();
                int chunkNo = chunkPair.getKey().getNumber();
                int sizeOfChunk = peerInfo.chunksStored.get(chunkPair.getKey()).getSize();
                peerInfo.chunksStored.remove(chunkPair.getKey());
                saveState();
                spaceToFree -= sizeOfChunk;

                FileUtils.deleteFile("../peers/peer" + peerInfo.peerID + "/chunks/" + fileKey + chunkNo);
                System.out.println("Deleted chunk " + chunkNo + " of file " + fileKey + " in the system");

                try {
                    byte[] header = MessageUtils.createHeader(MessageUtils.protocolVersion1, MessageUtils.MessageType.REMOVED, peerInfo.peerID, fileKey.getBytes(StandardCharsets.US_ASCII), chunkNo, -1);
                    MulticastSender.send(header, controlMultCast);
                } catch (IOException e) {
                    return "Failed to Communicate with Other Peers".getBytes();
                }
            }
        }
        saveState();
        return "Disk Space Updated".getBytes();
    }

    @Override
    public byte[] state() throws RemoteException {
        System.out.println("State");
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try {
            ObjectOutputStream objOutStream = new ObjectOutputStream(new BufferedOutputStream(byteStream));
            objOutStream.writeObject(peerInfo);
            objOutStream.close();
        } catch (IOException ignored) {
        }
        return byteStream.toByteArray();
    }

    private static boolean setupRMI() {
        PeerInterface peer = new Peer();
        Registry registry;
        PeerInterface stub;

        try {
            stub = (PeerInterface) UnicastRemoteObject.exportObject(peer, 0);
        } catch (RemoteException e) {
            System.err.println("Error exporting Peer");
            return false;
        }

        try {
            registry = LocateRegistry.getRegistry(1090);
        }  catch (RemoteException e) {
            System.err.println("Failed to Find Registry");
            return false;
        }
        try {
            registry.rebind(peerInfo.accessPoint, stub);
        } catch (RemoteException e) {
            System.err.println("Failed to bind");
            return false;
        }
        return true;
    }

    private static void createDirs() throws IOException {
        FileUtils.createDirectory("../peers/peer" + peerInfo.peerID);
        System.out.println("Directory peer" + peerInfo.peerID + " created!");
    }

    public synchronized static void saveState() {
        try {
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            ObjectOutputStream objOutStream = new ObjectOutputStream(new BufferedOutputStream(byteStream));
            objOutStream.writeObject(Peer.peerInfo);
            objOutStream.close();
            FileUtils.chunkToFile(byteStream.toByteArray(), "../peers/peer" + Peer.peerInfo.peerID + "/metadata");
        } catch (IOException e) {
            System.err.println("Failed to Save State");
        }

        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {
        }
    }

    private static void recoverState() {
        try {
            byte[] info = FileUtils.fileToChunk("../peers/peer" + peerInfo.peerID + "/metadata");
            ByteArrayInputStream byteStream = new ByteArrayInputStream(info);
            ObjectInputStream objInStream = new ObjectInputStream(new BufferedInputStream(byteStream));
            PeerInfo peerInfo = (PeerInfo) objInStream.readObject();
            objInStream.close();
            String newAccessPoint = Peer.peerInfo.accessPoint;
            Peer.peerInfo = peerInfo;
            Peer.peerInfo.accessPoint = newAccessPoint;
            System.err.println("State Recovered");
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("No State to Recover");
        }
    }

    private static void checkDelChunks() {
        ArrayList<String> fileKeys = new ArrayList<>();
        ArrayList<Integer> chunkNumbers = new ArrayList<>();
        for (ChunkKey chunkKey : Peer.peerInfo.chunksStored.keySet())
            if (!fileKeys.contains(chunkKey.getFileKey())) {
                fileKeys.add(chunkKey.getFileKey());
                chunkNumbers.add(chunkKey.getNumber());
            }

        for (int i = 0; i < fileKeys.size(); i++) {
            try {
                byte[] header = MessageUtils.createHeader(MessageUtils.protocolVersion2, MessageUtils.MessageType.STORED, Peer.peerInfo.peerID, fileKeys.get(i).getBytes(StandardCharsets.US_ASCII), chunkNumbers.get(i), -1);
                MulticastSender.send(header, controlMultCast);
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java Peer " +
            "<protocol version> " +
            "<peer ID> " +
            "<access point>" +
            "<control_multicastAddr> <port> " +
            "<backup_multicastAddr> <port> " +
            "<restore_multicastAddr> <port> ");
    }

    public static void main(String[] args) {

        if (args.length != 9) {
            printUsage();
            return;
        }

        try {
            protocolVersion = args[0];
            peerInfo = new PeerInfo(Integer.parseInt(args[1]), args[2]);
            controlMultCast[0] = args[3];
            controlMultCast[1] = args[4];
            backupMultCast[0] = args[5];
            backupMultCast[1] = args[6];
            restoreMultCast[0] = args[7];
            restoreMultCast[1] = args[8];

            if (!setupRMI())
                System.exit(-1);
            createDirs();

            recoverState();
            saveState();

            new Thread(new MulticastReceiver(controlMultCast)).start();
            new Thread(new MulticastReceiver(backupMultCast)).start();
            new Thread(new MulticastReceiver(restoreMultCast)).start();

            if (protocolVersion.equals(MessageUtils.protocolVersion2))
                checkDelChunks();

            System.out.println("Peer " + peerInfo.peerID + " running...");
        } catch (RemoteException e) {
            System.err.println("Error setting up RMI");
            e.printStackTrace();
        } catch (NumberFormatException e) {
            System.err.println("Invalid PeerID.");
        } catch (IOException e) {
            System.err.println("Error creating directories");
        }
    }
}
