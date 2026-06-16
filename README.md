# Distributed monitor


## Algorithm

### 1. Acquire lock

1. Increment and save timestamp
2. Insert your timestamp into processes FIFO queue
3. Wait until you are the first in queue
4. Send messages
5. Wait until everyone agrees

### 2. Release lock
1. Remove yourself from the queue
2. Send reponse back to everyone with lower ticket
3. Clear list with server responses


## Run program
compile:
```bash
mvn compile
```

run servers, set different ports:
```bash
mvn exec:java -Pserver -Dexec.args="1100"
```

run clients, set server ports:
```bash
mvn exec:java -Pclient -Dexec.args="1100"
```
