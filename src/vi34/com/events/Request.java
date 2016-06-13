package vi34.com.events;

/**
 * Created by vi34 on 12/06/16.
 */
public class Request implements Event {
    public static final String TYPE = "Request";
    String op;
    String args;
    public Request(String op, String args) {
        this.op = op;
        this.args = args;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
