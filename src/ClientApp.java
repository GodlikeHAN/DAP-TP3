import java.io.IOException;
import java.util.*;

/** Console client: first argument is client number 1..5 (default node = same number). */
public class ClientApp {
    private final int clientNo, preferredNode;
    private final String clientId;
    private final Scanner console=new Scanner(System.in);
    public ClientApp(int clientNo,int preferredNode) {
        if(clientNo<1 || clientNo>5 || preferredNode<1 || preferredNode>5) throw new IllegalArgumentException("IDs must be 1..5");
        this.clientNo=clientNo; this.preferredNode=preferredNode; this.clientId="client-"+clientNo;
    }
    private Map<String,String> call(String type,Integer value) {
        String req=UUID.randomUUID().toString(); // unchanged over retries
        Map<String,String> m=Json.obj("type",type,"client_id",clientId,"request_id",req);
        if(value!=null) m.put("value",String.valueOf(value));
        Map<String,String> lastError=Json.obj("type","ERROR","reason","NO_AVAILABLE_LEADER");
        for(int retry=0;retry<5;retry++) {
            int node=(preferredNode-1+retry)%5+1;
            try {
                Map<String,String> answer=Net.call(node,m,2800);
                String reason=Json.get(answer,"reason");
                if(reason.equals("NO_READY_LEADER") || reason.equals("LEADER_UNREACHABLE") ||
                    reason.equals("NOT_LEADER") || reason.equals("NO_QUORUM_PENDING")) {
                    lastError=answer; Thread.sleep(500); continue;
                }
                return answer;
            } catch(IOException e) { try{ Thread.sleep(400); }catch(InterruptedException interrupted){Thread.currentThread().interrupt();break;} }
            catch(InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
        return lastError;
    }
    private void command(String cmd) {
        String[] parts=cmd.trim().split("\\s+");
        if(parts.length==0 || parts[0].isBlank()) return;
        String action=parts[0].toLowerCase(Locale.ROOT);
        String type=switch(action) { case "join" -> "JOIN"; case "write" -> "WRITE"; case "guess" -> "GUESS"; case "state" -> "GET_STATE"; default -> ""; };
        if(type.isBlank()) {System.out.println("Commands: join | write 50 | guess 60 | state | quit"); return;}
        Integer value=null;
        if(action.equals("write") || action.equals("guess")) {
            if(parts.length!=2) { System.out.println("Usage: "+action+" <0..100>");return; }
            try { value=Integer.parseInt(parts[1]); }
            catch(NumberFormatException e) { System.out.println("Invalid number");return; }
        }
        System.out.println(Json.encode(call(type,value)));
    }
    public static void main(String[] args) {
        if(args.length<1 || args.length>3) {
            System.out.println("Usage: java ClientApp <clientNo 1..5> [nodeNo 1..5] [join|state|write=50|guess=60]");return;
        }
        int clientNo=Integer.parseInt(args[0]);
        int nodeNo=args.length>=2?Integer.parseInt(args[1]):clientNo;
        ClientApp client=new ClientApp(clientNo,nodeNo);
        if(args.length==3) {
            String action=args[2].replace('=',' ');
            client.command(action); return;
        }
        System.out.println("Client "+client.clientId+" connected via node "+nodeNo);
        client.command("join");
        System.out.println("Commands: write 50 (first client only), guess 60, state, quit");
        while(true) {
            System.out.print(client.clientId+"> ");
            if(!client.console.hasNextLine()) break;
            String line=client.console.nextLine().trim();
            if(line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) break;
            client.command(line);
        }
    }
}
