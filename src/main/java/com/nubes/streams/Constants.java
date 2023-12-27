package com.nubes.streams;

public interface Constants {
    String LOGIN_TOPIC = "login-records";
    String DURATION_TOPIC = "login-durations";
    String DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSXXX";
    String REFRESH_TOKEN_INTERVAL_KEY = "refresh.token.interval";
    String REFRESH_TOKEN_PATH_KEY = "refresh.token.path";
    String CONFIG_LOG_PATH_KEY = "config.log.path";
    String REFRESH_TOKEN = "refresh_token";
    String CONFIG_LOG_PATH = "/api/v1/account/info";
    String REFRESH_TOKEN_PATH = "/connect/token";
    String JSON_PROPERTIES_KEY = "Properties";
    String JSON_USERID_KEY = "UserId";
    String JSON_USERNAME_KEY = "Username";
    String JSON_PATH_KEY = "Path";
    String JSON_LOG_LEVEL_KEY = "LogLevel";
    String JSON_GRANT_TYPE_KEY = "GrantType";

    enum LOG_LEVEL{
        REQUEST("Request"),
        RESPONSE("Response");
        private final String name;
        LOG_LEVEL(String name) {
            this.name = name;
        }
        public String getName()
        {
            return this.name;
        }
    }

}
