import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by haosun on 11/25/17.
 */
public class SDFSServer {
    public static final int tcpPort = 3000;
    public static final FileManager fileManager = new FileManager();

    public static void init() {
        new Thread(() -> {
            try {
                ServerSocket mainSocket = new ServerSocket(tcpPort);
                while (true) {
                    Socket socket = mainSocket.accept();
                    handleFileOperationMessage(socket);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public static void handleFileOperationMessage(Socket socket) {
        new Thread(() -> {
            try {
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                FileOperationMessage requestMessage = (FileOperationMessage) ois.readObject();
                String action = requestMessage.getAction();
                if (action.equals("put")) {
                    //put a file, maybe an update
                    SDFSFile file = (SDFSFile) requestMessage.getData();
                    //store the file
                    fileManager.storeFile(file);
                    defaultRespond(socket);

                    //update file list
                    SDFSFile lightFile = new SDFSFile(file.getFileName(), file.getTimeStamp(), file.getSHA256(), file.getFileSize(), file.getLastModificationTime());
                    //if this is an update, remove the old version file from the list
                    fileManager.fileLists.get(Daemon.ID).removeIf((SDFSFile existedFile) ->
                            (existedFile.getFileName().equals(file.getFileName())));
                    fileManager.fileLists.get(Daemon.ID).add(lightFile);
                    fileManager.spreadFileOperationMessage(new FileOperationMessage("add", lightFile));
                } else if (action.equals("add")) {
                    //another node has a new file, update this node's file list
                    SDFSFile file = (SDFSFile) requestMessage.getData(); //this file only contains information, no file content
                    String sourceIP = socket.getInetAddress().getHostAddress();
                    //update file list, remove old version (if any), add new version
                    for (String ID : fileManager.fileLists.keySet()) {
                        if (ID.split("#")[1].equals(sourceIP)) {
                            fileManager.fileLists.get(ID).removeIf((SDFSFile existedFile) ->
                                    (existedFile.getFileName().equals(file.getFileName())));
                            fileManager.fileLists.get(ID).add(file);
                            break;
                        }
                    }
                    defaultRespond(socket);
                } else if (action.equals("get")) {
                    //get file
                    String fileName = (String) requestMessage.getData();
                    byte[] file = fileManager.readFile(fileName);
                    if (file != null && file.length != 0) {
                        respond(socket, new FileOperationMessage("ok", file));
                    }
                } else if (action.equals("delete")) {
                    String fileName = (String) requestMessage.getData();
                    fileManager.fileLists.get(Daemon.ID).removeIf((SDFSFile existedFile) ->
                            (existedFile.getFileName().equals(fileName)));
                    defaultRespond(socket);
                    fileManager.spreadFileOperationMessage(new FileOperationMessage("remove", fileName));
                } else if (action.equals("remove")) {
                    String fileName = (String) requestMessage.getData();
                    String sourceIP = socket.getInetAddress().getHostAddress();
                    for (String ID : fileManager.fileLists.keySet()) {
                        if (ID.split("#")[1].equals(sourceIP)) {
                            fileManager.fileLists.get(ID).removeIf((SDFSFile existedFile) ->
                                    (existedFile.getFileName().equals(fileName)));
                            break;
                        }
                    }
                    defaultRespond(socket);
                }
                socket.close();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void respond(Socket socket, FileOperationMessage response) throws IOException {
        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
        oos.writeObject(response);
        oos.flush();
        oos.close();
    }

    private static void defaultRespond(Socket socket) throws IOException {
        respond(socket, new FileOperationMessage("ok", null));
    }
}
