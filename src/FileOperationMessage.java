import java.io.Serializable;

/**file operation message
 * instruct nodes to do certain operation
 * may contain file contents in it
 * Created by haosun on 11/25/17.
 */
public class FileOperationMessage implements Serializable {
    private String action;
    private Object data;

    /**
     * constructor
     * @param action action
     * @param data data to send
     */
    public FileOperationMessage(String action, Object data) {
        this.action = action;
        this.data = data;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
