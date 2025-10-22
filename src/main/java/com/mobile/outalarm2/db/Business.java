package com.mobile.outalarm2.db;

import lombok.Data;

import java.util.Date;

@Data
public class Business {
    private String CREATE_TIME;
    private String PAYLOAD;
    private String REQUEST_PAYLOAD;
    private String RESPONSE_MESSAGE;
}
