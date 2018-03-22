import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**file operations
 * Created by haosun on 11/25/17.
 */
public class FileManager {
    public TreeMap<String, ArrayList<SDFSFile>> fileLists;

    /**
     * constructor
     */
    public FileManager() {
        this.fileLists = new TreeMap<>();
    }

    /**
     * file need to store at this node
     * @param file the file to store
     * @throws IOException io exception
     */
    public void storeFile(SDFSFile file) throws IOException {
        FileOutputStream fos = new FileOutputStream("files/" + file.getSHA256());
        fos.write(file.getFile());
        fos.close();
    }

    /**
     * file is stored at this node, need to read it
     * @param fileName file to read
     * @return byte[] of the file
     */
    public byte[] readFile(String fileName) {
        for (SDFSFile file : fileLists.get(Daemon.ID)) {
            if (file.getFileName().equals(fileName)) {
                try {
                    return Files.readAllBytes(Paths.get("files/" + file.getSHA256()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    /**
     * send file operation message to a target node
     * @param targetIP target ip
     * @param requestMessage request message
     * @return response message
     */
    public FileOperationMessage sendFileOperationMessage(String targetIP, FileOperationMessage requestMessage) {
        try {
            Socket socket = new Socket(targetIP, SDFSServer.tcpPort);
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(requestMessage);
            oos.flush();
            oos.close();
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            FileOperationMessage responseMessage = (FileOperationMessage) ois.readObject();
            ois.close();
            socket.close();
            return responseMessage;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * use sendFileOperationMessage to send the request message to all the nodes in the cluster
     * @param requestMessage request file operation message
     */
    public void spreadFileOperationMessage(FileOperationMessage requestMessage) {
        for (String ID : fileLists.keySet()) {
            String targetIP = ID.split("#")[1];
            new Thread(() -> {
                sendFileOperationMessage(targetIP, requestMessage);
            }).start();
        }
    }

    /**
     * pick number nodes in the cluster to put the file
     * @param list all possible nodes
     * @param number how many nodes to put
     * @return ID of chosen nodes
     */
    public ArrayList<String> randomPick(Collection<String> list, int number) {
        ArrayList<String> arrayList = new ArrayList<>(list);
        ArrayList<String> result = new ArrayList<>();
        while (number > 0 && arrayList.size() > 0) {
            int random = (int) (Math.random() * arrayList.size());
            result.add(arrayList.get(random));
            arrayList.remove(random);
            number--;
        }
        return result;
    }

    public ArrayList<String> listFileLocations(String fileName) {
        ArrayList<String> result = new ArrayList<>();
        for (String ID : fileLists.keySet()) {
            for (SDFSFile file : fileLists.get(ID)) {
                if (file.getFileName().equals(fileName)) {
                    result.add(ID);
                    break;
                }
            }
        }
        return result;
    }

    public void updateFileList(TreeMap<String, long[]> membershipList) {
        //if a node leave the cluster (leave of crash)
        //replicate its files to a new node
        //ensure there are 3 replications of each file in the cluster all the time
        boolean toReplicate = false;

        //if a node exist in membership list, but not in file list
        for (String ID : membershipList.keySet()) {
            SDFSServer.fileManager.fileLists.putIfAbsent(ID, new ArrayList<>());
        }

        //if a node exits in file list, but not in membership list
        //remove it from file list. replicate the files on it to a
        //new node
        ArrayList<String> removeList = new ArrayList<>();
        for (String ID : SDFSServer.fileManager.fileLists.keySet()) {
            if (membershipList.get(ID) == null) {
                toReplicate = true;
                removeList.add(ID);
            }
        }
        for (String toRemoveID : removeList) {
            fileLists.remove(toRemoveID);
            System.out.println("remove " + toRemoveID + "from file list");
        }

        if (toReplicate) {
            new Thread(() -> {
                try {
                    Thread.sleep(1500);
                    replicateToNewNode();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }

    public void replicateToNewNode() {
        //find and store all distinct files in the SDFS
        HashSet<String> fileNames = new HashSet<>();
        for (String ID : fileLists.keySet()) {
            for (SDFSFile file : fileLists.get(ID)) {
                fileNames.add(file.getFileName());
            }
        }

        for (String fileName : fileNames) {
            ArrayList<String> fileLocations = listFileLocations(fileName);
            if (fileLocations.size() < 3 && fileLocations.size() > 0) {
                Collections.sort(fileLocations);
                if (Daemon.ID.equals(fileLocations.get(0))) {
                    int replicationNum = 3 - fileLocations.size();
                    ArrayList<String> candidateHosts = new ArrayList<>();
                    for (String ID : fileLists.keySet()) {
                        if (!fileLocations.contains(ID)) {
                            candidateHosts.add(ID);
                        }
                    }
                    byte[] fileContent = readFile(fileName);
                    SDFSFile fileToReplicate = null;
                    for (SDFSFile tmpFile : fileLists.get(Daemon.ID)) {
                        if (tmpFile.getFileName().equals(fileName)) {
                            fileToReplicate = new SDFSFile(tmpFile, fileContent);
                            break;
                        }
                    }
                    if (fileToReplicate != null) {
                        for (String locationID : randomPick(candidateHosts, replicationNum)) {
                            String locationIP = locationID.split("#")[1];
                            FileOperationMessage response = sendFileOperationMessage(locationIP, new FileOperationMessage("put", fileToReplicate));
                            if (response == null || !response.getAction().equals("ok")) {
                                System.out.println("replicate " + fileName + " at " + locationID + " failed");
                            } else {
                                System.out.println("replicate " + fileName + " at " + locationID + " succeeded");
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * if the last modification is within 1 min of the current put operation
     * need confirmation from user whether current modification should be processed
     * @param fileName file to put
     * @return true : need confirmation
     *         false : doesn't need confirmation
     */
    public boolean needConfirmationPut(String fileName) {
        long lastModificationTime = 0;
        for (String ID : fileLists.keySet()) {
            for (SDFSFile file : fileLists.get(ID)) {
                if (file.getFileName().equals(fileName) && lastModificationTime < file.getLastModificationTime()) {
                    lastModificationTime = file.getLastModificationTime();
                }
            }
        }
        return (System.currentTimeMillis() - lastModificationTime) < (60 * 1000);
    }

    /**
     * put file to the cluster
     * if already exists, then update
     * @param localFile file at local node to put to the cluster
     * @param fileName SDFS file name
     */
    public void putFile(String localFile, String fileName) {
        int timeStamp = 0;
        //check whether the file already exist
        a: for (String ID : fileLists.keySet()) {
            for (SDFSFile file : fileLists.get(ID)) {
                if (file.getFileName().equals(fileName)) {
                    timeStamp = file.getTimeStamp() + 1;
                    break a;
                }
            }
        }

        //create new SDFS file
        SDFSFile file = null;
        try {
            file = new SDFSFile(fileName, localFile, timeStamp);
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        ArrayList<String> fileLocations = null;
        if (timeStamp > 0) {
            //file already exists, update file
            System.out.println(fileName + " already exists. this put is update");
            fileLocations = listFileLocations(fileName);
        } else {
            //new file, choose 3 nodes to put the file
            System.out.println(fileName + " is a new file. this put is create");
            fileLocations = randomPick(Daemon.membershipList.keySet(), 3);
        }
        for (String locationID : fileLocations) {
            String locationIP = locationID.split("#")[1];
            FileOperationMessage response = sendFileOperationMessage(locationIP, new FileOperationMessage("put", file));
            if (response == null || !response.getAction().equals("ok")) {
                System.out.println("Store " + fileName + " at " + locationID + " failed");
            } else {
                System.out.println("Store " + fileName + " at " + locationID + " succeeded");
            }
        }
    }

    /**
     * get file from cluster, and store it locally
     * @param localFile store as local file
     * @param fileName the file to get from the cluster
     */
    public void getFile(String localFile ,String fileName) {
        ArrayList<String> fileLocations = listFileLocations(fileName);
        if (fileLocations == null || fileLocations.size() == 0) {
            System.out.println("file : " + fileName + " not found");
            return;
        }
        Collections.shuffle(fileLocations);
        for (String locationID : fileLocations) {
            String locationIP = locationID.split("#")[1];
            FileOperationMessage response = sendFileOperationMessage(locationIP, new FileOperationMessage("get", fileName));
            if (response != null) {
                byte[] file = (byte[]) response.getData();
                try {
                    FileOutputStream fos = new FileOutputStream(localFile);
                    fos.write(file);
                    fos.flush();
                    fos.close();
                    System.out.println("file : " + fileName + "get succeeded");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return;
            }
        }
        System.out.println("file : " + fileName + "get failed");
    }

    /**
     * delete file from the cluster
     * @param fileName the file to delete
     */
    public void deleteFile(String fileName) {
        ArrayList<String> fileLocations = listFileLocations(fileName);
        for (String locationID : fileLocations) {
            String locationIP = locationID.split("#")[1];
            FileOperationMessage response = sendFileOperationMessage(locationIP, new FileOperationMessage("delete", fileName));
            if (response == null) {
                System.out.println("file : " + fileName + " at " + locationID + " delete failed");
            } else if (response.getAction().equals("ok")) {
                System.out.println("file : " + fileName + " at " + locationID + " delete succeeded");
            } else {
                System.out.println("file : " + fileName + " at " + locationID + " not found delete succeeded");
            }
        }
    }

    /**
     * list all the files in the cluster
     * @return description of all the files in the cluster
     */
    public String getClusterDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append("SDFS file list:\n");
        for (String ID : fileLists.keySet()) {
            sb.append("node : ").append(ID).append("\n");
            for (SDFSFile file : fileLists.get(ID)) {
                sb.append(file.getFileName()).append("size:").append(file.getFileSize()).append("last modification time").append(file.getLastModificationTime());
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    /**
     * list all the SDFS files at this node
     * @return string
     */
    public String getSelfDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append("SDFS file list at ").append(Daemon.ID).append("\n");
        for (SDFSFile file : fileLists.get(Daemon.ID)) {
            sb.append(file.getFileName()).append("size:").append(file.getFileSize()).append("last modification time").append(file.getLastModificationTime());
            sb.append("\n");
        }
        return sb.toString();
    }
}
