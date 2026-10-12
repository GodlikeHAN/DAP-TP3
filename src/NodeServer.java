import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Simplified ZooKeeper/ZAB-like 5-node coordinator. NOT production ZAB/Raft. */
public class NodeServer {
    private static final int NODES=5, QUORUM=3, HEARTBEAT_MS=500, TIMEOUT_MS=2000;
    private final int id;
    private final LogStore log;
    private final Path termFile;
    private final ExecutorService workers=Executors.newCachedThreadPool();
    private final ScheduledExecutorService timer=Executors.newScheduledThreadPool(3);
    private final AtomicBoolean electing=new AtomicBoolean(false);
    private final Object writeLock=new Object();
    private final Map<String, Guess> guesses=new HashMap<>();
    private record Guess(String clientId, int value, String requestId) {}
    private int guessRound=-1;
    private long firstGuessTime=0;

    private volatile int term, votedFor=-1, leaderId=-1;
    private volatile boolean ready=false;
    private volatile long lastHeartbeat=System.currentTimeMillis(), lastElectionAttempt=0, lastQuorum=0;
    private int nextSeq=0;

    public NodeServer(int nodeId) throws Exception {
        if (nodeId < 1 || nodeId > 5) throw new IllegalArgumentException("node ID must be 1..5");
        this.id=nodeId;
        Path data=Paths.get("data"); Files.createDirectories(data);
        termFile=data.resolve("node-"+id+"-term.json");
        log=new LogStore(data,id);
        if (Files.exists(termFile)) {
            Map<String,String> persisted=Json.decode(Files.readString(termFile));
            term=Json.num(persisted,"term"); votedFor=Json.num(persisted,"votedFor");
        }
        // A restarted node must NOT restart as leader, even if it used to be one.
        leaderId=-1;
    }
    private void persistTerm() throws IOException {
        LogStore.atomicWrite(termFile,Json.encode(Json.obj("term",term,"votedFor",votedFor))+"\n");
    }
    private synchronized void changeTerm(int newTerm, int vote) throws IOException {
        if (newTerm < term) throw new IOException("term regression");
        term=newTerm; votedFor=vote; leaderId=-1; ready=false; persistTerm();
    }
    private void info(String s) { System.out.printf("[Node %d] %s%n",id,s); }
    private static Map<String,String> ok(Object... x) {
        Map<String,String> m=Json.obj("type","RESULT","ok",true);
        m.putAll(Json.obj(x)); return m;
    }
    private static Map<String,String> err(String reason) { return Json.obj("type","ERROR","ok",false,"reason",reason); }
    private static boolean success(Map<String,String> m) { return "true".equals(Json.get(m,"ok")); }
    private LogStore.Entry latest() { return log.latestCommitted(); }
    private static int epoch(LogStore.Entry e) { return e==null?0:e.epoch(); }
    private static int seq(LogStore.Entry e) { return e==null?0:e.seq(); }
    private static int compare(int ep1,int sq1,int ep2,int sq2) {
        int c=Integer.compare(ep1,ep2); return c!=0?c:Integer.compare(sq1,sq2);
    }
    private boolean activeLeader() {
        return leaderId!=-1 && (leaderId==id || System.currentTimeMillis()-lastHeartbeat<TIMEOUT_MS);
    }
    private synchronized boolean acceptLeader(int sender,int senderTerm) throws IOException {
        if (senderTerm<term || (senderTerm==term && votedFor!=-1 && votedFor!=sender)) return false;
        if (senderTerm>term) changeTerm(senderTerm,sender);
        else if (votedFor==-1) { votedFor=sender; persistTerm(); }
        if (leaderId==id && sender!=id) ready=false;
        leaderId=sender; lastHeartbeat=System.currentTimeMillis();
        return true;
    }
    private boolean isLeader() { return leaderId==id && ready; }

    private Map<String,String> dispatch(Map<String,String> m) throws Exception {
        String type=Json.get(m,"type");
        switch (type) {
            case "ELECTION": {
                LogStore.Entry e=latest();
                return Json.obj("type","VOTE","from",id,"lastEpoch",epoch(e),"lastSeq",seq(e),
                        "term",term,"leader",leaderId,"activeLeader",activeLeader());
            }
            case "VOTE_REQUEST": {
                int candidate=Json.num(m,"from"), proposedTerm=Json.num(m,"term");
                int cEpoch=Json.num(m,"lastEpoch"), cSeq=Json.num(m,"lastSeq");
                synchronized(this) {
                    LogStore.Entry local=latest();
                    if (proposedTerm<term || (activeLeader() && leaderId!=candidate) ||
                            compare(cEpoch,cSeq,epoch(local),seq(local))<0)
                        return Json.obj("type","VOTE","ok",false,"term",term,"from",id);
                    if (proposedTerm>term) changeTerm(proposedTerm,-1);
                    if (votedFor==-1 || votedFor==candidate) {
                        votedFor=candidate; persistTerm(); lastHeartbeat=System.currentTimeMillis();
                        return Json.obj("type","VOTE","ok",true,"term",term,"from",id);
                    }
                    return Json.obj("type","VOTE","ok",false,"term",term,"from",id);
                }
            }
            case "LEADER_ANNOUNCE":
            case "HEARTBEAT": {
                int sender=Json.num(m,"from"), t=Json.num(m,"term");
                if (!acceptLeader(sender,t)) return err("STALE_LEADER");
                LogStore.Entry e=latest();
                return ok("from",id,"lastEpoch",epoch(e),"lastSeq",seq(e),"logSize",log.all().size());
            }
            case "SYNC_REQUEST": return ok("entries",LogStore.pack(log.all()),"term",term);
            case "SYNC_INSTALL": {
                int sender=Json.num(m,"from"), t=Json.num(m,"term");
                if (!acceptLeader(sender,t)) return err("STALE_SYNC");
                List<LogStore.Entry> incoming=LogStore.unpack(Json.get(m,"entries"));
                if (incoming.stream().anyMatch(e->!"COMMITTED".equals(e.status()))) return err("BAD_SNAPSHOT");
                log.install(incoming);
                return ok("installed",incoming.size());
            }
            case "PROPOSE": {
                int sender=Json.num(m,"from"), t=Json.num(m,"epoch");
                if (t!=term || !acceptLeader(sender,t)) return err("STALE_PROPOSAL");
                LogStore.Entry entry=new LogStore.Entry(t,Json.num(m,"seq"),Json.get(m,"op"),
                        Json.num(m,"value"),"PROPOSED",Json.get(m,"request_id"),
                        Json.get(m,"client_id"),Json.longNum(m,"time"));
                log.propose(entry); // persistent BEFORE ACK
                return Json.obj("type","ACK","ok",true,"from",id,"epoch",t,"seq",entry.seq());
            }
            case "COMMIT": {
                int t=Json.num(m,"epoch"), sender=Json.num(m,"from");
                if (t!=term || !acceptLeader(sender,t)) return err("STALE_COMMIT");
                if (!log.commit(t,Json.num(m,"seq"))) return err("MISSING_PROPOSAL");
                return ok("committed",true);
            }
            case "ROUND_RESULT": {
                info("ROUND_RESULT: round="+Json.get(m,"round")+" winner="+Json.get(m,"winner")+
                        " target="+Json.get(m,"value")); return ok();
            }
            case "GET_STATE": case "JOIN": case "WRITE": case "GUESS": case "FORWARD":
                return clientRequest(m);
            default: return err("UNKNOWN_MESSAGE_TYPE");
        }
    }
    private Map<String,String> clientRequest(Map<String,String> raw) throws Exception {
        String kind="FORWARD".equals(Json.get(raw,"type"))?Json.get(raw,"op"):Json.get(raw,"type");
        if (!isLeader()) {
            int leader=leaderId;
            if (leader<1 || leader==id) return err("NO_READY_LEADER");
            Map<String,String> forwarded=new LinkedHashMap<>(raw);
            forwarded.put("type","FORWARD"); forwarded.put("op",kind);
            try { return Net.call(leader,forwarded,1800); }
            catch (IOException e) { return err("LEADER_UNREACHABLE"); }
        }
        if (kind.equals("GET_STATE")) return stateReply();
        String client=Json.get(raw,"client_id"), req=Json.get(raw,"request_id");
        if (!client.matches("[a-zA-Z0-9_-]{1,40}") || req.isBlank() || req.length()>100)
            return err("INVALID_CLIENT_OR_REQUEST_ID");
        if ((kind.equals("WRITE")||kind.equals("GUESS")) && (Json.num(raw,"value")<0 || Json.num(raw,"value")>100))
            return err("VALUE_MUST_BE_0_TO_100");
        if (kind.equals("GUESS")) return recordGuess(client,req,Json.num(raw,"value"));
        synchronized(writeLock) {
            LogStore.Entry done=log.byRequest(req);
            if (done!=null && "COMMITTED".equals(done.status())) return ok("status","ALREADY_COMMITTED","epoch",done.epoch(),"seq",done.seq());
            GameState s=GameState.from(log.committed());
            if (kind.equals("JOIN")) {
                if (s.members().contains(client)) return ok("status","ALREADY_JOINED","role",s.members().get(0).equals(client)?"FIRST":"GUESSER","players",s.members().size());
                if (s.started()) return err("GAME_ALREADY_STARTED");
                if (s.members().size()>=5) return err("MAX_FIVE_CLIENTS");
                Map<String,String> response=transact("JOIN",0,req,client,System.currentTimeMillis());
                if (success(response)) response.putAll(Json.obj("role",s.members().isEmpty()?"FIRST":"GUESSER","players",s.members().size()+1));
                return response;
            }
            if (kind.equals("WRITE")) {
                if (s.members().isEmpty() || !s.members().get(0).equals(client)) return err("ONLY_FIRST_CLIENT_CAN_WRITE");
                if (s.target()!=null || s.started()) return err("INITIAL_TARGET_ALREADY_SET");
                Map<String,String> response=transact("WRITE",Json.num(raw,"value"),req,client,System.currentTimeMillis());
                if (success(response)) info("Initial target="+Json.num(raw,"value"));
                return response;
            }
            return err("INVALID_OPERATION");
        }
    }
    private Map<String,String> stateReply() {
        GameState s=GameState.from(log.committed());
        int count;
        synchronized(guesses) { count=(guessRound==s.round())?guesses.size():0; }
        LogStore.Entry e=latest();
        return ok("leader",id,"term",term,"lastEpoch",epoch(e),"lastSeq",seq(e),
                "started",s.started(),"target",s.target()==null?"unset":s.target(),
                "round",s.round(),"players",s.members().size(),"guesses",count,
                "members",String.join(",",s.members()));
    }
    private Map<String,String> recordGuess(String client,String req,int number) {
        GameState s=GameState.from(log.committed());
        if (!s.started() || s.target()==null) return err("GAME_NOT_STARTED");
        if (!s.members().contains(client) || s.members().get(0).equals(client)) return err("ONLY_OTHER_REGISTERED_CLIENTS_MAY_GUESS");
        synchronized(guesses) {
            if (guessRound!=s.round()) { guesses.clear(); guessRound=s.round(); firstGuessTime=0; }
            Guess old=guesses.get(client);
            if (old!=null) return old.requestId().equals(req)
                    ?ok("status","GUESS_ALREADY_ACCEPTED","round",s.round())
                    :err("ONE_GUESS_PER_PLAYER_PER_ROUND");
            guesses.put(client,new Guess(client,number,req));
            if (firstGuessTime==0) firstGuessTime=System.currentTimeMillis();
            info("Guess from "+client+" = "+number+" (round "+s.round()+")");
            return ok("status","GUESS_ACCEPTED","round",s.round(),"guesses",guesses.size());
        }
    }
    /** Serialize all write proposals. The leader counts itself as one ACK. */
    private Map<String,String> transact(String op,int value,String req,String client,long at) {
        synchronized(writeLock) {
            if (!isLeader()) return err("NOT_LEADER");
            try {
                LogStore.Entry entry=log.byRequest(req);
                if (entry!=null && entry.status().equals("COMMITTED"))
                    return ok("status","ALREADY_COMMITTED","epoch",entry.epoch(),"seq",entry.seq());
                if (entry!=null && entry.epoch()!=term) return err("OLD_PENDING_REQUEST_RETRY_AFTER_ELECTION");
                if (entry==null) {
                    entry=new LogStore.Entry(term,++nextSeq,op,value,"PROPOSED",req,client,at);
                    log.propose(entry);
                }
                final LogStore.Entry proposal=entry;
                CompletionService<Map<String,String>> completion=new ExecutorCompletionService<>(workers);
                int outstanding=0;
                Map<String,String> p=new LinkedHashMap<>(entry.json());
                p.put("type","PROPOSE"); p.put("from",String.valueOf(id));
                for (int other=1;other<=NODES;other++) if(other!=id) {
                    final int peer=other; outstanding++;
                    completion.submit(()-> {
                        try { return Net.call(peer,p,850); } catch(IOException e) { return err("PEER_DOWN"); }
                    });
                }
                int ack=1; // leader's own durable PROPOSE is its ACK
                long deadline=System.currentTimeMillis()+1100;
                while (outstanding>0 && ack<QUORUM) {
                    long wait=deadline-System.currentTimeMillis(); if (wait<=0) break;
                    Future<Map<String,String>> f=completion.poll(wait,TimeUnit.MILLISECONDS);
                    if (f==null) break; outstanding--;
                    Map<String,String> response=f.get();
                    if ("ACK".equals(Json.get(response,"type")) && success(response) &&
                            Json.num(response,"epoch")==proposal.epoch() && Json.num(response,"seq")==proposal.seq()) ack++;
                }
                if (ack<QUORUM) { info("No quorum for "+op+" tag="+entry.key()+" ACKs="+ack); return err("NO_QUORUM_PENDING"); }
                if (!isLeader() || term!=entry.epoch()) return err("LEADERSHIP_CHANGED_PENDING");
                log.commit(entry.epoch(),entry.seq());
                Map<String,String> commit=Json.obj("type","COMMIT","from",id,"epoch",entry.epoch(),"seq",entry.seq());
                broadcast(commit);
                info("COMMIT ("+entry.epoch()+","+entry.seq()+") "+op+"="+value+" ACK="+ack+"/5");
                return ok("status","COMMITTED","epoch",entry.epoch(),"seq",entry.seq(),"acks",ack);
            } catch (Exception e) { info("Commit error: "+e); return err("TRANSACTION_FAILED_"+e.getClass().getSimpleName()); }
        }
    }
    private void broadcast(Map<String,String> message) {
        for(int peer=1;peer<=NODES;peer++) if(peer!=id) {
            final int node=peer;
            workers.execute(()->{ try { Net.call(node,message,500); } catch (IOException ignored) {} });
        }
    }
    private void gameTick() {
        if (!isLeader()) return;
        GameState s=GameState.from(log.committed());
        if (!s.started() && s.target()!=null && !s.members().isEmpty() &&
                System.currentTimeMillis()-s.lastJoinTime()>=10000) {
            synchronized(writeLock) {
                s=GameState.from(log.committed());
                if(!s.started() && s.target()!=null && System.currentTimeMillis()-s.lastJoinTime()>=10000) {
                    Map<String,String> result=transact("START",0,"game-start","system",System.currentTimeMillis());
                    if(success(result)) info("GAME STARTED! Players="+s.members().size());
                }
            }
            return;
        }
        if (!s.started() || s.target()==null) return;
        Guess winner;
        synchronized(guesses) {
            if (guessRound!=s.round() || guesses.isEmpty()) return;
            boolean everyoneGuessed=guesses.size()>=s.members().size()-1;
            if(!everyoneGuessed && System.currentTimeMillis()-firstGuessTime<10000) return;
            int target=s.target();
            winner=guesses.values().stream().min(Comparator
                    .comparingInt((Guess x)->Math.abs(x.value()-target))
                    .thenComparingInt(Guess::value).thenComparing(Guess::clientId)).orElseThrow();
        }
        Map<String,String> result=transact("GUESS",winner.value(),"winner-r"+s.round(),winner.clientId(),System.currentTimeMillis());
        if(success(result)) {
            info("ROUND "+s.round()+" WINNER "+winner.clientId()+" guess="+winner.value());
            broadcast(Json.obj("type","ROUND_RESULT","round",s.round(),"winner",winner.clientId(),"value",winner.value()));
            synchronized(guesses) { guesses.clear(); guessRound=s.round()+1; firstGuessTime=0; }
        }
    }
    private void electionTick() {
        if (leaderId==id) return;
        if(System.currentTimeMillis()-lastHeartbeat<TIMEOUT_MS) return;
        if(System.currentTimeMillis()-lastElectionAttempt<900) return;
        if(!electing.compareAndSet(false,true)) return;
        lastElectionAttempt=System.currentTimeMillis();
        workers.execute(()->{try { runElection(); } finally {electing.set(false);} });
    }
    private void runElection() {
        try {
            List<Map<String,String>> survey=new ArrayList<>();
            LogStore.Entry own=latest();
            survey.add(Json.obj("type","VOTE","from",id,"lastEpoch",epoch(own),"lastSeq",seq(own),"term",term));
            for(int peer=1;peer<=NODES;peer++) if(peer!=id) {
                try {
                    Map<String,String> vote=Net.call(peer,Json.obj("type","ELECTION","from",id,"epoch",epoch(own),"seq",seq(own)),350);
                    if("VOTE".equals(Json.get(vote,"type"))) survey.add(vote);
                } catch(IOException ignored) {}
            }
            if(survey.size()<QUORUM) return;
            for(Map<String,String> v:survey) if("true".equals(Json.get(v,"activeLeader"))) {
                int who=Json.num(v,"leader"), t=Json.num(v,"term");
                if(who>0 && who!=id && t>=term) {
                    acceptLeader(who,t); return;
                }
            }
            Map<String,String> best=survey.stream().max((a,b)->{
                int cmp=compare(Json.num(a,"lastEpoch"),Json.num(a,"lastSeq"),Json.num(b,"lastEpoch"),Json.num(b,"lastSeq"));
                return cmp!=0?cmp:Integer.compare(Json.num(b,"from"),Json.num(a,"from"));
            }).orElseThrow();
            if(Json.num(best,"from")!=id) return; // wait for best candidate to begin its election
            int maxTerm=survey.stream().mapToInt(m->Json.num(m,"term")).max().orElse(term);
            int proposedTerm;
            synchronized(this) { proposedTerm=Math.max(term,maxTerm)+1; changeTerm(proposedTerm,id); }
            int votes=1;
            for(int peer=1;peer<=NODES;peer++) if(peer!=id) {
                try {
                    Map<String,String> response=Net.call(peer,Json.obj("type","VOTE_REQUEST","from",id,
                            "term",proposedTerm,"lastEpoch",epoch(own),"lastSeq",seq(own)),350);
                    if(success(response) && Json.num(response,"term")==proposedTerm) votes++;
                } catch(IOException ignored) {}
            }
            if(votes<QUORUM || term!=proposedTerm) return;
            leaderId=id; ready=false; lastHeartbeat=System.currentTimeMillis(); lastQuorum=lastHeartbeat;
            info("ELECTED epoch="+term+" votes="+votes+" (syncing logs...)");
            broadcast(Json.obj("type","LEADER_ANNOUNCE","from",id,"term",term));
            recover();
        } catch(Exception e) { info("Election error: "+e); }
    }
    private void recover() throws Exception {
        // Merge committed transactions. Replay orphan PROPOSED entries in a NEW epoch.
        List<LogStore.Entry> snapshots=new ArrayList<>(log.all());
        for (int peer=1;peer<=NODES;peer++) if(peer!=id) {
            try {
                Map<String,String> response=Net.call(peer,Json.obj("type","SYNC_REQUEST","from",id),650);
                if(success(response)) snapshots.addAll(LogStore.unpack(Json.get(response,"entries")));
            } catch(IOException ignored) {}
        }
        Comparator<LogStore.Entry> byTag=Comparator.comparingInt(LogStore.Entry::epoch).thenComparingInt(LogStore.Entry::seq);
        TreeMap<String,LogStore.Entry> merged=new TreeMap<>((a,b)->{
            String[] x=a.split(":"), y=b.split(":");
            int c=Integer.compare(Integer.parseInt(x[0]),Integer.parseInt(y[0]));
            return c!=0?c:Integer.compare(Integer.parseInt(x[1]),Integer.parseInt(y[1]));
        });
        for(LogStore.Entry e:snapshots) {
            LogStore.Entry prior=merged.get(e.key());
            if (prior!=null && (!prior.op().equals(e.op()) || prior.value()!=e.value() || !prior.requestId().equals(e.requestId())))
                throw new IOException("Conflicting log entry at "+e.key());
            if(prior==null || (e.status().equals("COMMITTED") && !prior.status().equals("COMMITTED"))) merged.put(e.key(),e);
        }
        List<LogStore.Entry> committed=merged.values().stream().filter(e->e.status().equals("COMMITTED")).sorted(byTag).toList();
        List<LogStore.Entry> pending=merged.values().stream().filter(e->e.status().equals("PROPOSED")).sorted(byTag).toList();
        log.install(committed);
        nextSeq=log.all().stream().filter(e->e.epoch()==term).mapToInt(LogStore.Entry::seq).max().orElse(0);
        if (!installQuorum(committed)) { info("Recovery cannot install majority; writes blocked"); return; }
        ready=true;
        info("SYNC completed: committed="+committed.size()+", orphan proposals="+pending.size());
        for(LogStore.Entry e:pending) {
            if(log.byRequest(e.requestId())!=null) continue;
            Map<String,String> r=transact(e.op(),e.value(),e.requestId(),e.clientId(),e.time());
            if(!success(r)) { ready=false; info("Could not recover proposal " + e.key()); return; }
        }
    }
    private boolean installQuorum(List<LogStore.Entry> entries) {
        Map<String,String> install=Json.obj("type","SYNC_INSTALL","from",id,"term",term,
                "entries",LogStore.pack(entries));
        int installed=1;
        for(int peer=1;peer<=NODES;peer++) if(peer!=id) {
            try { if(success(Net.call(peer,install,700))) installed++; }
            catch(IOException ignored) {}
        }
        return installed>=QUORUM;
    }
    private void heartbeatTick() {
        if(leaderId!=id) return;
        int successes=1;
        for(int peer=1;peer<=NODES;peer++) if(peer!=id) {
            try {
                Map<String,String> response=Net.call(peer,Json.obj("type","HEARTBEAT","from",id,"term",term),350);
                if(success(response)) {
                    successes++;
                    LogStore.Entry e=latest();
                    if(ready && (Json.num(response,"lastEpoch")!=epoch(e) || Json.num(response,"lastSeq")!=seq(e)
                            || Json.num(response,"logSize")!=log.all().size())) {
                        int remote=peer;
                        workers.execute(()-> {
                            try { Net.call(remote,Json.obj("type","SYNC_INSTALL","from",id,"term",term,
                                    "entries",LogStore.pack(log.committed())),1000); }
                            catch(IOException ignored) {}
                        });
                    }
                } else if (Json.num(response,"term")>term) changeTerm(Json.num(response,"term"),-1);
            } catch(Exception ignored) {}
        }
        if(successes>=QUORUM) lastQuorum=System.currentTimeMillis();
        if(System.currentTimeMillis()-lastQuorum>TIMEOUT_MS && leaderId==id) {
            info("LOST QUORUM; stepping down"); leaderId=-1; ready=false;
            lastHeartbeat=System.currentTimeMillis()-TIMEOUT_MS;
        }
    }
    public void start() throws Exception {
        ServerSocket listener=new ServerSocket(); listener.setReuseAddress(true);
        listener.bind(new InetSocketAddress("127.0.0.1",5000+id));
        info("FOLLOWER listening on 127.0.0.1:"+(5000+id)+" (persisted term="+term+")");
        timer.scheduleWithFixedDelay(this::electionTick,300,300,TimeUnit.MILLISECONDS);
        timer.scheduleWithFixedDelay(()-> workers.execute(this::heartbeatTick),400,HEARTBEAT_MS,TimeUnit.MILLISECONDS);
        timer.scheduleWithFixedDelay(()->workers.execute(()->{try {gameTick();} catch(Exception e){info("Game timer: "+e);}}),600,500,TimeUnit.MILLISECONDS);
        while(true) {
            Socket socket=listener.accept();
            workers.execute(()->{
                try(socket;
                    BufferedReader r=new BufferedReader(new InputStreamReader(socket.getInputStream(),StandardCharsets.UTF_8));
                    BufferedWriter w=new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(),StandardCharsets.UTF_8))) {
                    socket.setSoTimeout(2500);
                    String line=r.readLine(); if(line==null || line.length()>2_000_000) return;
                    Map<String,String> reply;
                    try { reply=dispatch(Json.decode(line)); }
                    catch(Exception e) { reply=err("SERVER_ERROR_"+e.getClass().getSimpleName()); info("Request error: "+e); }
                    w.write(Json.encode(reply)); w.write('\n'); w.flush();
                } catch(Exception ignored) {}
            });
        }
    }
    public static void main(String[] args) throws Exception {
        if(args.length!=1) { System.out.println("Usage: java NodeServer <1..5>"); return; }
        new NodeServer(Integer.parseInt(args[0])).start();
    }
}
