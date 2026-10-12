DAP TP3. Five Java nodes on 127.0.0.1 ports 5001 to 5005 share one log. One leader accepts writes. A write is kept only after 3 of the 5 nodes acknowledge it. Clients use that log to play a guessing game. Java 21 and Maven are required. Run the commands below from the project directory.

Compile with mvn -q compile.

Start each node in its own terminal: java -cp target/classes NodeServer 1, and the same for 2, 3, 4 and 5. Logs are written under data/ and are not part of the submission. Delete data/ for an empty log.

Start a client with java -cp target/classes ClientApp 1. The second number, if given, is the node to connect to. It defaults to the client number. The client joins immediately and stays connected. If that node is down, the next command tries the next nodes.

Commands are join, write 50, guess 60, state, quit and stop. write is only for the first player and sets the first target from 0 to 100. quit disconnects this client only. stop ends the game for everyone through the same proposal and majority commit as any other write.

At most five players can join. The first player sets the target and does not guess in round 1. The round opens 10 seconds after the last join. A round ends when every online player who must guess has guessed. The closest guess wins. The same distance goes to the smaller number, then to the smaller client id. Clients learn who won, but not the target or the winning number.

That guess becomes the next target. The winner waits on the next round and the other online players guess. For 15 seconds after a round, new players may join and guessing is closed. A guess already sent still counts if that player then disconnects. A player the leader no longer sees is not required.

A restarted node does not stay leader. If the leader sends no heartbeat for 2 seconds, the others elect again. The highest (epoch, seq) wins, and the smaller node id wins a tie. The new leader merges the logs, installs the committed entries on a majority, and replays proposals that were not committed. Heartbeats are every 500 ms. The leader steps down if it no longer has a majority.
