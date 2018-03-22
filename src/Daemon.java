import java.io.*;
import java.net.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by haosun on 11/1/17.
 * background thread of a node in the distributed system
 */
public class Daemon {
    //ID of the node, includes time stamp and IP address
    static String ID;

    //set using configuration file
    //well-known introducers in the distributed system
    private static String[] hostNames;
    //join port number of introducers, new nodes send join request to this port.
    //same for all the introducers.
    static int joinPortNumber;
    //nodes communicate using this port. same for all the nodes.
    //introducer will have a join port and a packet port
    static int packetPortNumber;

    //neighbours set, store ID of neighbours
    static final Set<String> neighbours = new HashSet<>();
    //membership list. key is ID, value is {heart beat counter, local time millis}
    static final TreeMap<String, long[]> membershipList = new TreeMap<>();

    //SDFSServer
    public static final SDFSServer sdfsServer = new SDFSServer();

    //use to write to log file
    private static PrintWriter fileOutput;

    /**
     * constructor
     * @param configPath path of configuration file
     */
    private Daemon(String configPath) {
        if (!(new File(configPath)).isFile()) {
            System.err.println("invalid configuration file path");
            System.exit(1);
        }

        Properties configuration = new Properties();
        try {
            //load config file
            InputStream inputStream = new FileInputStream(configPath);
            configuration.load(inputStream);
            hostNames = configuration.getProperty("hostNames").split(":");
            joinPortNumber = Integer.parseInt(configuration.getProperty("joinPortNumber"));
            packetPortNumber = Integer.parseInt(configuration.getProperty("packetPortNumber"));
            String logFilePath = configuration.getProperty("logFilePath");

            System.out.println("configuration file loaded");
            System.out.println("introducer host names are:");
            for (String hostName : hostNames) {
                System.out.println(hostName);
            }
            System.out.println("introducers listen join request on port : " + joinPortNumber);
            System.out.println("nodes communicate on port : " + packetPortNumber);

            //init ID
            //getInet4Address().toString() will return "/192.168.0.13" so we need substring
            ID = LocalDateTime.now().toString() + "#" + getInet4Address().toString().substring(1);

            //init log file output stream
            File outputFir = new File(logFilePath);
            if (!outputFir.exists()) {
                outputFir.mkdir();
            }
            fileOutput = new PrintWriter(new BufferedWriter(new FileWriter(logFilePath + "result.log")));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * update neighbours of a node. The node will send heart beats to its neighbours.
     * Every node has one successor and one predecessor
     */
    static void updateNeighbours() {

        synchronized (membershipList) {
            synchronized (neighbours) {
                neighbours.clear();
                //get predecessor
                String currentKey;
                currentKey = membershipList.lowerKey(ID);
                if (currentKey == null) {
                    currentKey = membershipList.lastKey();
                }
                if (!currentKey.equals(ID)) {
                    neighbours.add(currentKey);
                }

                //get successor
                currentKey = membershipList.higherKey(ID);
                if (currentKey == null) {
                    currentKey = membershipList.firstKey();
                }
                if (!currentKey.equals(ID)) {
                    neighbours.add(currentKey);
                }

                for (String neighbour : neighbours) {
                    long[] neighbourDetail = new long[]{membershipList.get(neighbour)[0], System.currentTimeMillis()};
                    membershipList.put(neighbour, neighbourDetail);
                }
            }
        }
    }

    /**
     * new node join the group.
     * @param isIntroducer whether the node is an introducer node
     */
    private static void joinGroup(boolean isIntroducer) {
        //init socket, will bind to some automatically chosen port
        DatagramSocket nodeSocket = null;
        while (nodeSocket == null) {
            try {
                nodeSocket = new DatagramSocket();
            } catch (SocketException e) {
                e.printStackTrace();
            }
        }

        //send ID to introducer
        byte[] sendData = ID.getBytes();
        for (String hostName : hostNames) {
            try {
                InetAddress inetAddress = InetAddress.getByName(hostName);
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, inetAddress, joinPortNumber);
                nodeSocket.send(sendPacket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //receive membership list from introducer, put them into local membership list
        byte[] receiveData = new byte[1024];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        try {
            nodeSocket.setSoTimeout(2000);
            nodeSocket.receive(receivePacket);
            String responseFromIntroducer = new String(receivePacket.getData(), 0, receivePacket.getLength());
            String[] members = responseFromIntroducer.split("%");
            for (String member : members) {
                String[] memberDetail = member.split("/");
                long[] counterLocalTime = new long[]{Long.parseLong(memberDetail[1]), System.currentTimeMillis()};
                membershipList.put(memberDetail[0], counterLocalTime);
            }

            //update neighbours
            updateNeighbours();

            //write log
            writeLog("JOIN", ID);
        } catch (SocketTimeoutException e) {
            if (!isIntroducer) {
                System.err.println("all introducers are down");
                System.exit(1);
            } else {
                System.out.println("you are the first introducer");
                membershipList.put(ID, new long[]{0, System.currentTimeMillis()});
                //SDFS added
                SDFSServer.fileManager.fileLists.putIfAbsent(ID, new ArrayList<>());

                writeLog("JOIN", ID);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Inet4Address getInet4Address() throws UnknownHostException {
        try {
            Enumeration<NetworkInterface> networkInterfaceEnumeration = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface networkInterface : Collections.list(networkInterfaceEnumeration)) {
                Enumeration<InetAddress> inetAddressEnumeration = networkInterface.getInetAddresses();
                for (InetAddress inetAddress : Collections.list(inetAddressEnumeration)) {
                    if (inetAddress instanceof Inet4Address && !inetAddress.isLoopbackAddress()) {
                        return (Inet4Address) inetAddress;
                    }
                }
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }
        return (Inet4Address) InetAddress.getLocalHost();
    }

    /**
     * display prompts to the user
     */
    private static void displayPrompt() {
        System.out.println("===============================");
        System.out.println("Please input the commands:.....");
        System.out.println("Enter \"JOIN\" to join to group......");
        System.out.println("Enter \"LEAVE\" to leave the group");
        System.out.println("Enter \"ID\" to show self's ID");
        System.out.println("Enter \"MEMBER\" to show the membership list");
        System.out.println("Enter \"NEIGHBOUR\" to show the neighbour list");
        System.out.println("Enter \"put local-file-path SDFS-file-name\" to put or update file to SDFS");
        System.out.println("Enter \"get SDFS-file-name local-file-path\" to get SDFS file to local path");
        System.out.println("Enter \"delete SDFS-file-name\" to delete SDFS file");
        System.out.println("Enter \"list cluster\" list all the SDFS file information");
        System.out.println("Enter \"list local SDFS\" list all the SDFS file information at local machine");
    }

    /**
     * write log files
     * @param action the action that is performed
     *               all possible actions:
     *               JOIN in daemon thread, node join the group
     *               INITIAL ADD in introducer thread
     *               HEARTBEAT OWN in heartbeat thread
     *               HEARTBEAT NEIGHBOR in listening thread
     *               HEARTBEAT REJOIN in listening thread
     *               GOSSIP ADD in listening thread
     *               GOSSIP LEAVE in listening thread
     *               GOSSIP REMOVE in listening thread
     *               PASS  in monitor thread
     *               FAILURE in monitor thread
     * @param nodeID the node ID
     */
    static void writeLog(String action, String nodeID) {

        // write logs about action happened to the nodeID into log
        fileOutput.println(LocalDateTime.now().toString() + " \"" + action + "\" " + nodeID);
        if (action.equals("JOIN") || action.equals("INITIAL ADD") || action.equals("HEARTBEAT REJOIN") ||
                action.equals("GOSSIP ADD") || action.equals("GOSSIP LEAVE") || action.equals("GOSSIP REMOVE")
                || action.equals("FAILURE")) {
            fileOutput.println("Updated Membership List:");
            for (String key : membershipList.keySet()) {
                fileOutput.println(key);
            }
            fileOutput.println("Updated Neighbor List:");
            for (String key : neighbours) {
                fileOutput.println(key);
            }
            fileOutput.println("======================");
        }
        fileOutput.flush();
    }

    public static void main(String[] args) {
        boolean isIntroducer = false;
        String configPath = null;
        if (args.length < 1 || args.length > 2) {
            System.err.println("invalid arguments number. please enter in format : <config_file_path> <-i>(optional)");
            System.exit(1);
        } else if (args.length == 1) {
            configPath = args[0];
        } else {
            configPath = args[0];
            if (!args[1].equals("-i")) {
                System.err.println("invalid argument.");
                System.err.println("invalid arguments number. please enter in format : <config_file_path> <-i>(optional)");
                System.exit(1);
            } else {
                isIntroducer = true;
                System.out.println("set this node as introducer.");
            }
        }

        Daemon daemon = new Daemon(configPath);

        displayPrompt();

        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in))) {
            String cmd;
            while ((cmd = bufferedReader.readLine()) != null) {
                if (cmd.equals("JOIN")) {
                    if (membershipList.size() == 0) {
                        System.out.println("join the group");
                        joinGroup(isIntroducer);
                        ExecutorService executorService = Executors.newFixedThreadPool(3 + (isIntroducer ? 1 : 0));
                        if (isIntroducer) {
                            executorService.execute(new IntroducerThread());
                        }
                        executorService.execute(new HeartbeatThread(900));
                        executorService.execute(new ListeningThread());
                        executorService.execute(new MonitorThread());
                    } else {
                        System.out.println("already in the group");
                    }

                } else if (cmd.equals("LEAVE")) {
                    System.out.println("leave the group");
                    if (membershipList.size() != 0) {
                        Protocol.sendGossip(ID, "LEAVE", membershipList.get(ID)[0],
                                2, 2, new DatagramSocket());
                        fileOutput.println(LocalDateTime.now().toString() + " \"LEAVE!!\" " + ID);
                        fileOutput.close();
                        System.exit(0);
                    }

                } else if (cmd.equals("ID")) {
                    System.out.println("Node ID : " + ID);

                } else if (cmd.equals("MEMBER")) {
                    System.out.println("membership list :");
                    System.out.println("=======================================");
                    for (Map.Entry<String, long[]> entry : membershipList.entrySet()) {
                        System.out.println("ID : " + entry.getKey() + "counter : "
                                + entry.getValue()[0] + "local time : " + entry.getValue()[1]);
                    }
                    System.out.println("=======================================");

                } else if (cmd.equals("NEIGHBOUR")) {
                    System.out.println("neighbour list :");
                    System.out.println("=======================================");
                    for (String neighbour : neighbours) {
                        System.out.println(neighbour);
                    }
                    System.out.println("=======================================");

                } else if (cmd.startsWith("put ") && cmd.split(" ").length == 3) {
                    String localFile = cmd.split(" ")[1];
                    String filename = cmd.split(" ")[2];
                    final boolean[] confirm = {true};
                    if (SDFSServer.fileManager.needConfirmationPut(filename)) {
                        System.out.print("Last update is within 1 min, are you sure to update? y/n: ");
                        if (bufferedReader.readLine() != null) {
                            String input = bufferedReader.readLine();
                            if (!input.startsWith("y")) {
                                System.out.println("Rejected by user.");
                                confirm[0] = false;
                            } else {
                                confirm[0] = true;
                            }
                        }
                        new Thread(() -> {
                            try {
                                Thread.sleep(30000);
                                if (!confirm[0]){
                                    System.out.println("\nAutomatically rejected after 30s");
                                    confirm[0] = false;
                                }
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }).start();
                    }
                    if (confirm[0]){
                        System.out.println("Putting " + localFile + " to " + filename);
                        SDFSServer.fileManager.putFile(localFile, filename);
                    }
                } else if (cmd.startsWith("get ") && cmd.split(" ").length == 3) {
                    String localFile = cmd.split(" ")[2];
                    String filename = cmd.split(" ")[1];
                    SDFSServer.fileManager.getFile(localFile, filename);
                } else if (cmd.startsWith("delete ") && cmd.split(" ").length == 2) {
                    String filename = cmd.split(" ")[1];
                    SDFSServer.fileManager.deleteFile(filename);
                } else if (cmd.equals("list cluster")) {
                    System.out.println(SDFSServer.fileManager.getClusterDescription());
                } else if (cmd.equals("list local SDFS")) {
                    System.out.println(SDFSServer.fileManager.getSelfDescription());
                } else {
                    System.out.println("unsupported command");
                }
                displayPrompt();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
