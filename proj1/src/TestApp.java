import utils.*;

import java.io.*;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;

public class TestApp {

    private static PeerInterface peer;

    private static byte[] backup(String[] args) throws RemoteException {
        if (args.length != 4) {
            System.out.println("Command Usage: java TestApp <peer_ap> BACKUP <file_name> <replication_degree>");
            return null;
        }

        int repDegree;
        try {
            repDegree = Integer.parseInt(args[3]);
        } catch (NumberFormatException e) {
            System.err.println("Replication degree must be an integer value between 1-9");
            return null;
        }
        if (repDegree > 9 || repDegree < 1) {
            System.err.println("Replication degree must be an integer value between 1-9");
            return null;
        }
        return peer.backup(args[2], repDegree);
    }

    private static byte[] restore(String[] args) throws RemoteException {
        if (args.length != 3) {
            System.out.println("Command Usage: java TestApp <peer_ap> RESTORE <file_name>");
            return null;
        }
        return peer.restore(args[2]);
    }

    private static byte[] delete(String[] args) throws RemoteException {
        if (args.length != 3) {
            System.out.println("Command Usage: java TestApp <peer_ap> DELETE <file_name>");
            return null;
        }
        return peer.delete(args[2]);
    }

    private static byte[] reclaim(String[] args) throws RemoteException {
        if (args.length != 3) {
            System.out.println("Command Usage: java TestApp <peer_ap> RECLAIM <disk_space>");
            return null;
        }
        int diskSpace;
        try {
            diskSpace = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.err.println("Disk space must be a non-negative integer");
            return null;
        }
        if (diskSpace < 0) {
            System.err.println("Disk space must be a non-negative integer");
            return null;
        }
        return peer.reclaim(diskSpace);
    }

    private static byte[] state(String[] args) throws RemoteException {
        if (args.length != 2) {
            System.out.println("Command Usage: java TestApp <peer_ap> STATE");
            return null;
        }
        return peer.state();
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 4) {
            System.out.println("Usage: java TestApp <peer_ap> <sub_protocol> <opnds>*");
            return;
        }

        String peerAccessPoint = args[0];

        try {
            Registry registry = LocateRegistry.getRegistry(1090);
            peer = (PeerInterface) registry.lookup(peerAccessPoint);
        } catch (RemoteException e) {
            System.err.println("Unable to Find Registry");
            return;
        } catch (NotBoundException e) {
            System.err.println("Peer non-existent");
            return;
        }

        try {
            String subProtocol = args[1];
            byte[] response;

            switch (subProtocol) {
                case "BACKUP": {
                    response = backup(args);
                    break;
                }
                case "RESTORE": {
                    response = restore(args);
                    break;
                }
                case "DELETE": {
                    response = delete(args);
                    break;
                }
                case "RECLAIM": {
                    response = reclaim(args);
                    break;
                }
                case "STATE": {
                    response = state(args);
                    break;
                }
                default: {
                    System.err.println("Sub-protocol non-existent");
                    return;
                }
            }

            if (response == null) {
                return;
            }

            if (subProtocol.equals("STATE")) {
                ByteArrayInputStream byteStream = new ByteArrayInputStream(response);
                ObjectInputStream objInStream = new ObjectInputStream(new BufferedInputStream(byteStream));
                PeerInfo peerInfo = (PeerInfo) objInStream.readObject();
                objInStream.close();

                System.out.println("ID: " + peerInfo.peerID + "\tAccess Point: " + peerInfo.accessPoint);
                System.out.println("Files Stored:");
                for (String key : peerInfo.filesStored.keySet()) {
                    System.out.println(
                            "\n\tFile Pathname: " + peerInfo.filesStored.get(key).getFileName() +
                            "\n\tFile ID: " + key +
                            "\n\tDesired Replication Degree: " + peerInfo.filesStored.get(key).getDesiredRepDegree());
                    for (int i = 0; i < peerInfo.filesStored.get(key).chunks.size(); i++) {
                        ArrayList<Integer> chunk = peerInfo.filesStored.get(key).chunks.get(i);
                        System.out.println(
                            "\n\t\tChunk Identifier: " + key + i +
                            "\n\t\tPerceived Replication Degree: " + chunk.size()
                        );
                    }
                }
                System.out.println("----------------------");
                System.out.println("Chunks Stored:");
                for (ChunkKey key : peerInfo.chunksStored.keySet())
                    System.out.println(
                        "\n\tChunk Identifier: " + key.getFileKey() + key.getNumber() +
                        "\n\tSize: " + peerInfo.chunksStored.get(key).getSize()/1000.0 + " KBytes" +
                        "\n\tDesired Replication Degree: " + peerInfo.chunksStored.get(key).getDesiredRepDegree() +
                        "\n\tPerceived Replication Degree: " + peerInfo.chunksStored.get(key).getPerceivedRepDegree()
                    );
                System.out.println("----------------------");
                System.out.println("Storage capacity: " + (peerInfo.getBackupSpace() != -1 ? peerInfo.getBackupSpace()/1000.0 + " KBytes" : "Unlimited") );
                System.out.println("Storage used: " + peerInfo.getOccupiedSpace()/1000.0 + " KBytes");
            } else {
                String responseString = new String(response).replace("\0","");
                System.out.println(responseString);
            }

        } catch (RemoteException e) {
            System.err.println("Peer offline");
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Error serializing Peer");
        }
    }
}
