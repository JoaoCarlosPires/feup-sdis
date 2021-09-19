package distributedbackup.filesystem;


import distributedbackup.protocol.BackupInitiator;
import distributedbackup.protocol.RestoreInitiator;
import distributedbackup.service.Peer;

import java.io.*;
import java.util.ArrayList;

public class LocalFile extends LocalFileInfo {
    private final Peer peer;

    public LocalFile(String filepath, int repDegree, Peer peer){
        super(filepath, repDegree, peer.getId());
        this.peer = peer;
    }

    public LocalFile(LocalFileInfo localInfo, Peer peer){
        super(localInfo.getFilepath(), localInfo.getRepDegree(), peer.getId());
        this.peer = peer;
        this.chunks = localInfo.getChunks();
    }

    public void backup(){
        File file = new File(this.filepath);
        FileInputStream fileStream;
        try{
            fileStream = new FileInputStream(file);
        }catch(Exception e){
            System.out.println("Unable to read " + this.filepath + " reason:" + e.getMessage());
            return;
        }

        int bytesRemaining = (int)file.length(), chunkNo = 0;
        while(bytesRemaining > 0){
            int chunkSize = Math.min(Chunk.MAX_SIZE, bytesRemaining);
            byte[] chunkData = new byte[chunkSize];

            try{
                bytesRemaining -= fileStream.read(chunkData, 0, chunkSize);
            }catch(Exception e){
                System.out.println("Failed to read chunk reason: " + e.getMessage());
                return;
            }
            LocalChunk lcChunk = new LocalChunk(this.fileID, chunkNo++, this.repDegree);
            this.chunks.add(lcChunk);

            BackupInitiator backupInitiator = new BackupInitiator(this.peer, lcChunk, chunkData);
            this.peer.threadPool.execute(backupInitiator);
        }

        if (file.length() % Chunk.MAX_SIZE == 0){
            LocalChunk lcChunk = new LocalChunk(this.fileID, chunkNo, this.repDegree);
            this.chunks.add(lcChunk);
            BackupInitiator backupInitiator = new BackupInitiator(this.peer, lcChunk, new byte[0]);
            this.peer.threadPool.execute(backupInitiator);
        }

        try{
            fileStream.close();
        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }

        peer.state.saveLocalFile(this);
    }

    public void restore(){
        ArrayList<Thread> threads = new ArrayList<>();

        for (LocalChunk chunk: this.getChunks()){
            Thread t = new Thread(new RestoreInitiator(this.peer, chunk));
            threads.add(t);
            t.start();
        }

        for (Thread t : threads){
            try {
                t.join();
            } catch (Exception e){}
        }

      if (this.isRestored()){
        this.restoreToFile();
        this.deleteCache();
      }
    }

    public void restoreToFile(){
        String path = this.peer.state.getPeerFileSystemFolder() + "/restored/" + this.filepath;
        File f = new File(path);
        f.getParentFile().mkdirs();
        try {
            f.createNewFile();
            FileOutputStream fos = new FileOutputStream(f, true);
            for (int i = 0; i < this.getChunkCount(); i++)
                fos.write(this.getChunk(i).getData());

            fos.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void deleteCache(){
        for (LocalChunk chunk : this.chunks)
            chunk.setData(null);
    }

    public Peer getPeer() {
        return peer;
    }

    public boolean isRestored(){
        for (Chunk chunk : this.chunks){
            if (chunk != null && chunk.getData() == null)
                return false;
        }
        return true;
    }
}
