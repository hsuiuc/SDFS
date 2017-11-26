import java.net.DatagramSocket;
import java.net.SocketException;

/**send heartbeat periodically
 * Created by haosun on 11/3/17.
 */
public class HeartbeatThread extends Thread{
    private int interval; //interval between two heartbeats
    private long counter;  //heartbeat counter

    HeartbeatThread(int interval) {
        //super("HeartbeatThread");
        this.interval = interval;
        this.counter = 1;
    }

    @Override
    public void run() {
        DatagramSocket sendSocket = null;
        try {
            sendSocket = new DatagramSocket();
        } catch (SocketException e) {
            e.printStackTrace();
        }

        while (true) {
            try {
                Thread.sleep(interval);
                synchronized (Daemon.membershipList) {
                    Protocol.sendHeartBeat(Daemon.ID, counter++, sendSocket);
                    Daemon.membershipList.put(Daemon.ID, new long[]{counter, System.currentTimeMillis()});
                    Daemon.writeLog("HEARTBEAT OWN", Daemon.ID);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
