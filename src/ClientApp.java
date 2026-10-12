import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** Console client. Interactive mode keeps the socket open so the node can push round events. */
public class ClientApp {
    private final int clientNo, preferredNode;
    private final String clientId;
    private final Scanner console=new Scanner(System.in);
    private final Object writeLock=new Object();
    private final Object consoleLock=new Object();
    private final LinkedBlockingQueue<Map<String,String>> replies=new LinkedBlockingQueue<>();
    private Socket socket;
    private BufferedWriter out;
    private int currentNode=-1, generation=0;

    public ClientApp(int clientNo,int preferredNode) {
        if(clientNo<1 || clientNo>5 || preferredNode<1 || preferredNode>5) throw new IllegalArgumentException("IDs must be 1..5");
        this.clientNo=clientNo; this.preferredNode=preferredNode; this.clientId="client-"+clientNo;
    }

    /** One request, one short connection. Used by the optional third command-line argument. */
    private Map<String,String> callOnce(String type,Integer value) {
        String req=UUID.randomUUID().toString();
        Map<String,String> m=Json.obj("type",type,"client_id",clientId,"request_id",req);
        if(value!=null) m.put("value",String.valueOf(value));
        Map<String,String> lastError=Json.obj("type","ERROR","ok",false,"reason","NO_AVAILABLE_LEADER");
        for(int retry=0;retry<5;retry++) {
            int node=(preferredNode-1+retry)%5+1;
            try {
                Map<String,String> answer=Net.call(node,m,2800);
                String reason=Json.get(answer,"reason");
                if(retryable(reason)) { lastError=answer; Thread.sleep(500); continue; }
                return answer;
            } catch(IOException e) { try{ Thread.sleep(400); }catch(InterruptedException interrupted){Thread.currentThread().interrupt();break;} }
            catch(InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
        return lastError;
    }

    private boolean connect(int node) {
        closeSocket();
        try {
            Socket s=new Socket();
            s.connect(new InetSocketAddress("127.0.0.1",5000+node),400);
            s.setSoTimeout(0);
            BufferedReader reader=new BufferedReader(new InputStreamReader(s.getInputStream(),StandardCharsets.UTF_8));
            BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(s.getOutputStream(),StandardCharsets.UTF_8));
            int gen=++generation;
            socket=s; out=writer; currentNode=node;
            replies.clear();
            Thread t=new Thread(()->readerLoop(reader,gen),"client-"+clientNo);
            t.setDaemon(true);
            t.start();
            return true;
        } catch(IOException e) { return false; }
    }
    private void closeSocket() {
        generation++;
        Socket old=socket; socket=null; out=null; currentNode=-1;
        if(old!=null) try { old.close(); } catch(IOException ignored) {}
    }
    private boolean connected() { return socket!=null && !socket.isClosed(); }
    private void readerLoop(BufferedReader reader,int gen) {
        try {
            while(gen==generation) {
                String line=reader.readLine();
                if(line==null || gen!=generation) break;
                Map<String,String> message=Json.decode(line);
                if(gen!=generation) break;
                String type=Json.get(message,"type");
                if(type.equals("RESULT") || type.equals("ERROR")) replies.offer(message);
                else printEvent(message);
            }
        } catch(Exception ignored) {}
    }
    private void printEvent(Map<String,String> message) {
        String type=Json.get(message,"type");
        String color=switch(type) {
            case "ROUND_RESULT" -> "\u001B[33m";
            case "COUNTDOWN" -> "\u001B[34m";
            case "ROUND_OPEN" -> "\u001B[32m";
            case "GAME_RESET" -> "\u001B[31m";
            default -> "\u001B[37m";
        };
        String text=switch(type) {
            case "ROUND_RESULT" -> Json.get(message,"winner").equals(clientId)
                    ? "You win."
                    : "Round finished.";
            case "COUNTDOWN" -> "The next round starts in 15 seconds. You may join now.";
            case "ROUND_OPEN" -> Json.get(message,"waiter").equals(clientId)
                    ? "Wait for the other clients to guess."
                    : "Please guess.";
            case "GAME_RESET" -> "Game cleared. Type join to start again.";
            default -> Json.encode(message);
        };
        synchronized(consoleLock) {
            System.out.println();
            System.out.println(color+"  "+text+"\u001B[0m");
            prompt();
        }
    }
    private void prompt() {
        System.out.print("\u001B[90m"+clientId+"> \u001B[0m");
        System.out.flush();
    }
    private void show(String action, Map<String,String> answer) {
        synchronized(consoleLock) {
            if(!"true".equals(Json.get(answer,"ok"))) {
                System.out.println("\u001B[31m  FAIL  "+explain(Json.get(answer,"reason"))+"\u001B[0m");
                return;
            }
            if(action.equals("state")) { printState(answer); return; }
            System.out.println("\u001B[32m  "+describe(action, answer)+"\u001B[0m");
            String hint=Json.get(answer,"hint");
            if(!hint.isBlank()) System.out.println("\u001B[33m  "+hint+"\u001B[0m");
        }
    }
    private String describe(String action, Map<String,String> answer) {
        String role=Json.get(answer,"role"), players=Json.get(answer,"players");
        String tag="("+Json.get(answer,"epoch")+","+Json.get(answer,"seq")+")  ACK "+Json.get(answer,"acks")+"/5";
        return switch(action) {
            case "join" -> "REJOINED".equals(Json.get(answer,"status"))
                    ? "OK    back online   role "+role+"   players "+players
                    : "OK    joined as "+role+"   players "+players+"   "+tag;
            case "write" -> "OK    target set   "+tag;
            case "guess" -> "GUESS_ACCEPTED".equals(Json.get(answer,"status"))
                    ? "OK    guess in   round "+Json.get(answer,"round")+"   "+Json.get(answer,"guesses")+"/"+Json.get(answer,"need")+" online"
                    : "OK    guess already counted   round "+Json.get(answer,"round");
            case "stop" -> "OK    stop committed   "+tag;
            case "quit" -> "OK    this client is offline   the game continues";
            default -> "OK    "+Json.get(answer,"status");
        };
    }
    private void printState(Map<String,String> answer) {
        String onlineNames=Json.get(answer,"onlineMembers");
        String members=Json.get(answer,"members");
        System.out.println("\u001B[33m  "+Json.get(answer,"hint")+"\u001B[0m");
        System.out.println("\u001B[36m  phase     "+Json.get(answer,"phase"));
        System.out.println("  round     "+Json.get(answer,"round"));
        System.out.println("  leader    node "+Json.get(answer,"leader"));
        System.out.println("  players   "+Json.get(answer,"players")+"    online "+Json.get(answer,"online")
                +"  "+(onlineNames.isBlank()?"-":onlineNames));
        System.out.println("  members   "+(members.isBlank()?"-":members));
        System.out.println("  guesses   "+Json.get(answer,"guesses")
                +"    countdown "+Json.get(answer,"countdown")+"s\u001B[0m");
    }
    private static String explain(String reason) {
        return switch(reason) {
            case "JOIN_ONLY_BETWEEN_ROUNDS" -> "join is open before the start and during the 10s countdown";
            case "WAIT_FOR_OTHERS" -> "Wait for the other clients to guess.";
            case "NOT_A_PLAYER" -> "Join the game before guessing.";
            case "ONLY_OTHER_REGISTERED_CLIENTS_MAY_GUESS" -> "Wait for the other clients to guess.";
            case "ONLY_FIRST_CLIENT_CAN_WRITE" -> "only the first player can set the target";
            case "ROUND_COUNTDOWN" -> "countdown is running   wait for the next round";
            case "GAME_NOT_STARTED" -> "the game has not started yet";
            case "INITIAL_TARGET_ALREADY_SET" -> "the target is already set";
            case "ONE_GUESS_PER_PLAYER_PER_ROUND" -> "you already guessed this round";
            case "NO_QUORUM_PENDING" -> "not enough nodes acknowledged   try again";
            case "NO_READY_LEADER", "LEADER_UNREACHABLE", "NOT_LEADER" -> "leader is not ready   retrying";
            case "NO_AVAILABLE_LEADER" -> "no node answered";
            case "VALUE_MUST_BE_0_TO_100" -> "value must be from 0 to 100";
            case "MAX_FIVE_CLIENTS" -> "the game already has 5 players";
            case "TIMEOUT" -> "no reply   the node may be down";
            case "" -> "request failed";
            default -> reason;
        };
    }
    private Map<String,String> call(String type,Integer value) {
        String req=UUID.randomUUID().toString();
        Map<String,String> message=Json.obj("type",type,"client_id",clientId,"request_id",req);
        if(value!=null) message.put("value",String.valueOf(value));
        Map<String,String> lastError=Json.obj("type","ERROR","ok",false,"reason","NO_AVAILABLE_LEADER");
        for(int retry=0;retry<5;retry++) {
            if(!connected()) {
                int node=(preferredNode-1+retry)%5+1;
                if(!connect(node)) { sleep(400); continue; }
            }
            try {
                replies.clear();
                synchronized(writeLock) {
                    out.write(Json.encode(message));
                    out.write('\n');
                    out.flush();
                }
                Map<String,String> answer=replies.poll(2800,TimeUnit.MILLISECONDS);
                if(answer==null) { lastError=Json.obj("type","ERROR","ok",false,"reason","TIMEOUT"); closeSocket(); continue; }
                if(retryable(Json.get(answer,"reason"))) { lastError=answer; closeSocket(); sleep(500); continue; }
                return answer;
            } catch(IOException e) { closeSocket(); sleep(400); }
            catch(InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
        return lastError;
    }
    private static boolean retryable(String reason) {
        return reason.equals("NO_READY_LEADER") || reason.equals("LEADER_UNREACHABLE") ||
                reason.equals("NOT_LEADER") || reason.equals("NO_QUORUM_PENDING");
    }
    private static void sleep(int ms) {
        try { Thread.sleep(ms); } catch(InterruptedException e) { Thread.currentThread().interrupt(); }
    }
    private void command(String cmd, boolean once) {
        String[] parts=cmd.trim().split("\\s+");
        if(parts.length==0 || parts[0].isBlank()) return;
        String action=parts[0].toLowerCase(Locale.ROOT);
        String type=switch(action) {
            case "join" -> "JOIN"; case "write" -> "WRITE"; case "guess" -> "GUESS";
            case "state" -> "GET_STATE"; case "stop" -> "STOP"; case "quit" -> "QUIT";
            default -> "";
        };
        if(type.isBlank()) {
            System.out.println("Commands: join | write 50 | guess 60 | state | stop | quit");
            return;
        }
        Integer value=null;
        if(action.equals("write") || action.equals("guess")) {
            if(parts.length!=2) { System.out.println("Usage: "+action+" <0..100>"); return; }
            try { value=Integer.parseInt(parts[1]); }
            catch(NumberFormatException e) { System.out.println("Invalid number"); return; }
        }
        show(action, once?callOnce(type,value):call(type,value));
    }
    public static void main(String[] args) {
        if(args.length<1 || args.length>3) {
            System.out.println("Usage: java ClientApp <clientNo 1..5> [nodeNo 1..5] [join|state|write=50|guess=60|stop]");
            return;
        }
        int clientNo=Integer.parseInt(args[0]);
        int nodeNo=args.length>=2?Integer.parseInt(args[1]):clientNo;
        ClientApp client=new ClientApp(clientNo,nodeNo);
        if(args.length==3) {
            client.command(args[2].replace('=',' '), true);
            return;
        }
        System.out.println("\u001B[1m"+client.clientId+"\u001B[0m   via node "+nodeNo);
        System.out.println("  join    write 50    guess 60    state    stop    quit");
        System.out.println("  quit closes only this client.  stop ends the game for everyone.");
        client.command("join", false);
        while(true) {
            synchronized(client.consoleLock) { client.prompt(); }
            if(!client.console.hasNextLine()) break;
            String line=client.console.nextLine().trim();
            if(line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
                client.command("quit", false);
                break;
            }
            client.command(line, false);
        }
        client.closeSocket();
    }
}
