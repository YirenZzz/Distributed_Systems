# Cloud Database System


## Start the ECS by running the following command:
```
java -jar ecs.jar
```

## Start five servers using the following commands:
```
java -jar server.jar -a localhost -p 30000 
java -jar server.jar -a localhost -p 30001 
java -jar server.jar -a localhost -p 30002 
java -jar server.jar -a localhost -p 30003 
java -jar server.jar -a localhost -p 30004 
```
## Start the client by running the following command:
```
java -jar client.jar
```

## The instructions below outline the commands utilized by the client terminal.

Connect client to one of the server (e.g. server with port 30000):
```
connect localhost 30000
```

### To Use The Transaction Feature:
```
beginTX
put key1 value1
put key2 value2
put key3 value3
commitTX
```
The user can type rollback during transaction to abort:
```
rollback
```

### To Use The Notification Mechanism:
```
sub <regex_key>
unsub <regex_key>
```
```<regex_key>``` is the regular expression of keys to subscribe. 
For example, using ```sub k.*```, the command line interface will display the subscription result of all matching keys start with 'k'. 

Notification will be printed to the standard output.

### To Get All Keys Matching a Pattern
Similar to the notification mechanism, this feature also supports matching keys using regular expressions. 
```
getAllKeys <regex_key>
```

### To Reuse a Previous Command:
The user can type
```
/<num_commands>
```
where ```<num_commands>``` is the number of commands to retrospect. 
For example, ```/2``` would reuse the second last command that the user inputted.
