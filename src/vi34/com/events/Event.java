package vi34.com.events;

import vi34.com.Connection;

/**
 * Created by vi34 on 12/06/16.
 */
public interface Event {
    String getType();
    Connection getConnection();
}
