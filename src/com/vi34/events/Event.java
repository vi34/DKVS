package com.vi34.events;

import com.vi34.Connection;

/**
 * Created by vi34 on 12/06/16.
 */
public interface Event {
    String getType();
    Connection getConnection();
}
