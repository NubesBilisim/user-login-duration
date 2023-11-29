java -Dlog4j.configuration=file:config/log4j.properties \
-cp user-login-duration-1.0-SNAPSHOT.jar:lib/* com.nubes.streams.UserLoginDuration \
config/prod.properties &
echo $! > .pid
