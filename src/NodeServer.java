import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class NodeServer {
    private static final int NODES=5, QUORUM=3, HEARTBEAT_MS=500, TIMEOUT_MS=2000, INTERMISSION_MS=10_000, ROUND_GAP_MS=15_000;
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
    /** Local client sockets. Online status is the live session, not log membership. */
    private final Map<String, ClientSession> sessions=new ConcurrentHashMap<>();
    /** Follower-reported online clients, keyed by node id. Rebuilt from heartbeats. */
    private final Map<Integer, Set<String>> onlineByNode=new ConcurrentHashMap<>();
    private final Map<Integer, Integer> heartbeatMisses=new ConcurrentHashMap<>();
    private int openedForRound=-1;
    private int followed=-1;

    private final class ClientSession {
        final String clientId;
        final Socket socket;
        final BufferedWriter out;
        final Object lock=new Object();
        ClientSession(String clientId, Socket socket, BufferedWriter out) {
            this.clientId=clientId; this.socket=socket; this.out=out;
        }
        void send(Map<String,String> message) throws IOException {
            synchronized(lock) {
                out.write(Json.encode(message));
                out.write('\n');
                out.flush();
            }
        }
        void close() { try { socket.close(); } catch(IOException ignored) {} }
    }

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
    private static final DateTimeFormatter CLOCK=DateTimeFormatter.ofPattern("HH:mm:ss");
    private final Object logLock=new Object();
    private void info(String tag, String message) {
        String color=switch(tag) {
            case "COMMIT","SYNC","LISTEN","ACK" -> "\u001B[36m";
            case "ELECT","VOTE" -> "\u001B[35m";
            case "WIN","GAME" -> "\u001B[33m";
            case "GUESS","ONLINE" -> "\u001B[32m";
            case "WAIT" -> "\u001B[34m";
            case "WARN" -> "\u001B[31m";
            default -> "\u001B[37m";
        };
        synchronized(logLock) {
            System.out.printf("%s%s  N%d  %-6s  %s\u001B[0m%n", color, LocalTime.now().format(CLOCK), id, tag, message);
            System.out.flush();
        }
    }
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
                info("ELECT", "survey from N"+Json.num(m,"from"));
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
                            compare(cEpoch,cSeq,epoch(local),seq(local))<0) {
                        info("VOTE", "no   candidate N"+candidate+"   term "+proposedTerm);
                        return Json.obj("type","VOTE","ok",false,"term",term,"from",id);
                    }
                    if (proposedTerm>term) changeTerm(proposedTerm,-1);
                    if (votedFor==-1 || votedFor==candidate) {
                        votedFor=candidate; persistTerm(); lastHeartbeat=System.currentTimeMillis();
                        info("VOTE", "yes   candidate N"+candidate+"   term "+term);
                        return Json.obj("type","VOTE","ok",true,"term",term,"from",id);
                    }
                    info("VOTE", "no   candidate N"+candidate+"   already voted N"+votedFor);
                    return Json.obj("type","VOTE","ok",false,"term",term,"from",id);
                }
            }
            case "LEADER_ANNOUNCE":
            case "HEARTBEAT": {
                int sender=Json.num(m,"from"), t=Json.num(m,"term");
                if (!acceptLeader(sender,t)) return err("STALE_LEADER");
                if (followed!=sender) {
                    followed=sender;
                    info("ELECT", "following N"+sender+"   epoch "+t);
                }
                LogStore.Entry e=latest();
                return ok("from",id,"lastEpoch",epoch(e),"lastSeq",seq(e),"logSize",log.all().size(),
                        "clients",String.join(",",sessions.keySet()));
            }
            case "SYNC_REQUEST": return ok("entries",LogStore.pack(log.all()),"term",term);
            case "SYNC_INSTALL": {
                int sender=Json.num(m,"from"), t=Json.num(m,"term");
                if (!acceptLeader(sender,t)) return err("STALE_SYNC");
                List<LogStore.Entry> incoming=LogStore.unpack(Json.get(m,"entries"));
                if (incoming.stream().anyMatch(e->!"COMMITTED".equals(e.status()))) return err("BAD_SNAPSHOT");
                log.install(incoming);
                info("SYNC", "installed "+incoming.size()+"   from N"+sender);
                return ok("installed",incoming.size());
            }
            case "PROPOSE": {
                int sender=Json.num(m,"from"), t=Json.num(m,"epoch");
                if (t!=term || !acceptLeader(sender,t)) return err("STALE_PROPOSAL");
                LogStore.Entry entry=new LogStore.Entry(t,Json.num(m,"seq"),Json.get(m,"op"),
                        Json.num(m,"value"),"PROPOSED",Json.get(m,"request_id"),
                        Json.get(m,"client_id"),Json.longNum(m,"time"));
                log.propose(entry); // persistent BEFORE ACK
                info("ACK", String.format("(%d,%d)  %-16s  to N%d", entry.epoch(), entry.seq(),
                        opDetail(entry.op(), entry.value(), entry.clientId()), sender));
                return Json.obj("type","ACK","ok",true,"from",id,"epoch",t,"seq",entry.seq());
            }
            case "COMMIT": {
                int t=Json.num(m,"epoch"), seq=Json.num(m,"seq"), sender=Json.num(m,"from");
                if (t!=term || !acceptLeader(sender,t)) return err("STALE_COMMIT");
                if (!log.commit(t,seq)) return err("MISSING_PROPOSAL");
                LogStore.Entry entry=log.byTag(t,seq);
                if (entry!=null) {
                    info("COMMIT", String.format("(%d,%d)  %-16s  from N%d", t, seq,
                            opDetail(entry.op(), entry.value(), entry.clientId()), sender));
                    logTarget(entry.op(), entry.value());
                }
                return ok("committed",true);
            }
            case "CLIENT_EVENT": {
                int sender=Json.num(m,"from"), t=Json.num(m,"term");
                if (!acceptLeader(sender,t)) return err("STALE_LEADER");
                Map<String,String> event=new LinkedHashMap<>(m);
                event.put("type",Json.get(m,"event"));
                event.remove("event"); event.remove("from"); event.remove("term");
                logEvent(event);
                deliverLocal(event);
                return ok();
            }
            case "PRESENCE": {
                if (leaderId!=id) return err("NOT_LEADER");
                int from=Json.num(m,"from");
                String client=Json.get(m,"client_id");
                Set<String> set=onlineByNode.computeIfAbsent(from,k->ConcurrentHashMap.newKeySet());
                if ("true".equals(Json.get(m,"online"))) set.add(client);
                else set.remove(client);
                info("ONLINE", client+("true".equals(Json.get(m,"online"))?" online":" offline")+"   via node "+from);
                return ok();
            }
            case "QUIT": return ok("status","OFFLINE");
            case "GET_STATE": case "JOIN": case "WRITE": case "GUESS": case "STOP": case "FORWARD":
                return clientRequest(m);
            default: return err("UNKNOWN_MESSAGE_TYPE");
        }
    }
    private static boolean isSessionMessage(String type) {
        return switch(type) {
            case "JOIN","WRITE","GUESS","GET_STATE","QUIT","STOP" -> true;
            default -> false;
        };
    }
    private Map<String,String> clientRequest(Map<String,String> raw) throws Exception {
        String kind="FORWARD".equals(Json.get(raw,"type"))?Json.get(raw,"op"):Json.get(raw,"type");
        if (!isLeader()) {
            int leader=leaderId;
            if (leader<1 || leader==id) return err("NO_READY_LEADER");
            info("FWD", kind+"   "+Json.get(raw,"client_id")+"   to N"+leader);
            Map<String,String> forwarded=new LinkedHashMap<>(raw);
            forwarded.put("type","FORWARD"); forwarded.put("op",kind);
            try { return Net.call(leader,forwarded,1800); }
            catch (IOException e) { return err("LEADER_UNREACHABLE"); }
        }
        if (kind.equals("GET_STATE")) return stateReply(Json.get(raw,"client_id"));
        String client=Json.get(raw,"client_id"), req=Json.get(raw,"request_id");
        if (!client.matches("[a-zA-Z0-9_-]{1,40}") || req.isBlank() || req.length()>100)
            return err("INVALID_CLIENT_OR_REQUEST_ID");
        if ((kind.equals("WRITE")||kind.equals("GUESS")) && (Json.num(raw,"value")<0 || Json.num(raw,"value")>100))
            return err("VALUE_MUST_BE_0_TO_100");
        if (kind.equals("GUESS")) return recordGuess(client,req,Json.num(raw,"value"));
        synchronized(writeLock) {
            LogStore.Entry done=log.byRequest(req);
            if (done!=null && "COMMITTED".equals(done.status())) return ok("status","ALREADY_COMMITTED","epoch",done.epoch(),"seq",done.seq());
            List<LogStore.Entry> committed=log.committed();
            GameState s=GameState.from(committed);
            if (kind.equals("STOP")) {
                Map<String,String> response=transact("STOP",0,req,client,System.currentTimeMillis());
                if (success(response)) {
                    synchronized(guesses) { guesses.clear(); guessRound=-1; }
                    openedForRound=-1;
                    pushEvent(Json.obj("type","GAME_RESET","status","RESET"));
                }
                return response;
            }
            if (kind.equals("JOIN")) {
                if (s.members().contains(client))
                    return ok("status","REJOINED","role",s.members().get(0).equals(client)?"FIRST":"GUESSER",
                            "players",s.members().size(),"hint",hintFor(client,s,committed));
                if (s.started() && countdownLeft(committed)<=0) return err("JOIN_ONLY_BETWEEN_ROUNDS");
                if (s.members().size()>=5) return err("MAX_FIVE_CLIENTS");
                Map<String,String> response=transact("JOIN",0,req,client,System.currentTimeMillis());
                if (success(response)) {
                    GameState now=GameState.from(log.committed());
                    response.putAll(Json.obj("role",now.members().isEmpty()||now.members().get(0).equals(client)?"FIRST":"GUESSER",
                            "players",now.members().size(),"hint",hintFor(client,now,log.committed())));
                }
                return response;
            }
            if (kind.equals("WRITE")) {
                if (s.members().isEmpty() || !s.members().get(0).equals(client)) return err("ONLY_FIRST_CLIENT_CAN_WRITE");
                if (s.target()!=null || s.started()) return err("INITIAL_TARGET_ALREADY_SET");
                Map<String,String> response=transact("WRITE",Json.num(raw,"value"),req,client,System.currentTimeMillis());
                if (success(response)) response.put("hint", "Waiting for the round to open.");
                return response;
            }
            return err("INVALID_OPERATION");
        }
    }
    private Map<String,String> stateReply(String client) {
        List<LogStore.Entry> committed=log.committed();
        GameState s=GameState.from(committed);
        int count;
        synchronized(guesses) { count=(guessRound==s.round())?guesses.size():0; }
        LogStore.Entry e=latest();
        long left=countdownLeft(committed);
        Set<String> online=onlineMembers(s);
        String phase=!s.members().isEmpty() && s.target()==null ? "WAITING_TARGET"
                : !s.started() ? (s.target()==null ? "LOBBY" : "STARTING")
                : left>0 ? "COUNTDOWN" : "GUESSING";
        return ok("leader",id,"term",term,"lastEpoch",epoch(e),"lastSeq",seq(e),
                "started",s.started(),"round",s.round(),"players",s.members().size(),"guesses",count,
                "members",String.join(",",s.members()),"phase",phase,
                "countdown",(left+999)/1000,"waiter",roundWaiter(s,committed),
                "online",online.size(),"onlineMembers",String.join(",",online),
                "hint",hintFor(client,s,committed));
    }
    private Map<String,String> recordGuess(String client,String req,int number) {
        List<LogStore.Entry> committed=log.committed();
        GameState s=GameState.from(committed);
        if (!s.started() || s.target()==null) return err("GAME_NOT_STARTED");
        if (countdownLeft(committed)>0) return err("ROUND_COUNTDOWN");
        if (!s.members().contains(client)) return err("NOT_A_PLAYER");
        if (client.equals(roundWaiter(s,committed))) return err("WAIT_FOR_OTHERS");
        synchronized(guesses) {
            if (guessRound!=s.round()) { guesses.clear(); guessRound=s.round(); }
            Guess old=guesses.get(client);
            if (old!=null) return old.requestId().equals(req)
                    ?ok("status","GUESS_ALREADY_ACCEPTED","round",s.round())
                    :err("ONE_GUESS_PER_PLAYER_PER_ROUND");
            guesses.put(client,new Guess(client,number,req));
            int need=requiredGuessers(s,committed).size();
            info("GUESS", client+" = "+number+"   round "+s.round()+"   "+guesses.size()+"/"+need+" online");
            return ok("status","GUESS_ACCEPTED","round",s.round(),"guesses",guesses.size(),"need",need,
                    "hint","Waiting for the other clients to guess.");
        }
    }
    /** How many times the game has been reset. Keeps START/winner ids unique across games. */
    private static int gameGeneration(List<LogStore.Entry> committed) {
        int stops=0;
        for (LogStore.Entry e:committed) if ("STOP".equals(e.op())) stops++;
        return stops;
    }
    /** Milliseconds left in the post-round join window. Later JOIN entries do not cancel it. */
    private long countdownLeft(List<LogStore.Entry> committed) {
        for (int i=committed.size()-1;i>=0;i--) {
            LogStore.Entry entry=committed.get(i);
            if ("STOP".equals(entry.op()) || "START".equals(entry.op()) || "WRITE".equals(entry.op())) return 0;
            if ("GUESS".equals(entry.op())) return Math.max(0, entry.time()+ROUND_GAP_MS-System.currentTimeMillis());
        }
        return 0;
    }
    private Set<String> onlineClientIds() {
        Set<String> all=new HashSet<>(sessions.keySet());
        for (Set<String> peers:onlineByNode.values()) all.addAll(peers);
        return all;
    }
    /** Online players who must guess. Round 1 waits on the first player; later rounds wait on the last winner. */
    private Set<String> requiredGuessers(GameState s, List<LogStore.Entry> committed) {
        String waiter=roundWaiter(s,committed);
        Set<String> online=onlineClientIds();
        Set<String> required=new LinkedHashSet<>();
        for (String member:s.members()) if (!member.equals(waiter) && online.contains(member)) required.add(member);
        return required;
    }
    private String roundWaiter(GameState s, List<LogStore.Entry> committed) {
        if (s.members().isEmpty()) return "";
        if (s.round()<=1) return s.members().get(0);
        String winner=latestGuessWinner(committed);
        return winner.isEmpty()?s.members().get(0):winner;
    }
    private static String latestGuessWinner(List<LogStore.Entry> committed) {
        for (int i=committed.size()-1;i>=0;i--) {
            LogStore.Entry entry=committed.get(i);
            if ("STOP".equals(entry.op()) || "WRITE".equals(entry.op())) return "";
            if ("GUESS".equals(entry.op())) return entry.clientId();
        }
        return "";
    }
    private String hintFor(String client, GameState s, List<LogStore.Entry> committed) {
        if (s.members().isEmpty() || !s.members().contains(client)) return "Type join to enter the game.";
        if (s.target()==null) return s.members().get(0).equals(client)
                ? "Please set the target."
                : "Waiting for the first player to set the target.";
        if (!s.started() || countdownLeft(committed)>0) return "Waiting for the next round.";
        return client.equals(roundWaiter(s,committed))
                ? "Wait for the other clients to guess."
                : "Please guess.";
    }
    private Set<String> onlineMembers(GameState s) {
        Set<String> online=onlineClientIds();
        Set<String> members=new LinkedHashSet<>();
        for (String member:s.members()) if (online.contains(member)) members.add(member);
        return members;
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
                if (ack<QUORUM) { info("WARN", "no quorum for "+op+" ("+entry.epoch()+","+entry.seq()+")   ACK "+ack+"/5"); return err("NO_QUORUM_PENDING"); }
                if (!isLeader() || term!=entry.epoch()) return err("LEADERSHIP_CHANGED_PENDING");
                log.commit(entry.epoch(),entry.seq());
                Map<String,String> commit=Json.obj("type","COMMIT","from",id,"epoch",entry.epoch(),"seq",entry.seq());
                broadcast(commit);
                String detail=opDetail(op,value,client);
                info("COMMIT", String.format("(%d,%d)  %-16s  ACK %d/5", entry.epoch(), entry.seq(), detail, ack));
                logTarget(op, value);
                return ok("status","COMMITTED","epoch",entry.epoch(),"seq",entry.seq(),"acks",ack);
            } catch (Exception e) { info("WARN", "commit failed   "+e.getClass().getSimpleName()); return err("TRANSACTION_FAILED_"+e.getClass().getSimpleName()); }
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
        synchronized(writeLock) {
            if (!isLeader()) return;
            List<LogStore.Entry> committed=log.committed();
            GameState s=GameState.from(committed);
            if (!s.started() && s.target()!=null && !s.members().isEmpty() &&
                    System.currentTimeMillis()-s.lastJoinTime()>=INTERMISSION_MS) {
                if(!s.started() && s.target()!=null && System.currentTimeMillis()-s.lastJoinTime()>=INTERMISSION_MS) {
                    int generation=gameGeneration(committed);
                    Map<String,String> result=transact("START",0,"game-start-"+generation,"system",System.currentTimeMillis());
                    if(success(result) && "COMMITTED".equals(Json.get(result,"status"))) {
                        info("GAME", "started   players "+s.members().size());
                        openedForRound=s.round();
                        pushEvent(Json.obj("type","ROUND_OPEN","round",s.round(),"waiter",roundWaiter(s,log.committed())));
                    }
                }
                return;
            }
            if (countdownLeft(committed)>0) return;
            if (s.started() && s.target()!=null && openedForRound!=s.round()) {
                openedForRound=s.round();
                pushEvent(Json.obj("type","ROUND_OPEN","round",s.round(),"waiter",roundWaiter(s,committed)));
            }
            if (!s.started() || s.target()==null) return;
            Guess winner;
            synchronized(guesses) {
                if (guessRound!=s.round()) { guesses.clear(); guessRound=s.round(); }
                Set<String> required=requiredGuessers(s,committed);
                if (required.isEmpty() || !guesses.keySet().containsAll(required)) return;
                int target=s.target();
                winner=guesses.values().stream().min(Comparator
                        .comparingInt((Guess x)->Math.abs(x.value()-target))
                        .thenComparingInt(Guess::value).thenComparing(Guess::clientId)).orElseThrow();
            }
            int finished=s.round();
            int generation=gameGeneration(committed);
            Map<String,String> result=transact("GUESS",winner.value(),"winner-g"+generation+"-r"+finished,winner.clientId(),System.currentTimeMillis());
            if(success(result) && "COMMITTED".equals(Json.get(result,"status"))) {
                pushEvent(Json.obj("type","ROUND_RESULT","round",finished,"winner",winner.clientId()));
                synchronized(guesses) { guesses.clear(); guessRound=finished+1; }
                pushEvent(Json.obj("type","COUNTDOWN","seconds",15,"round",finished,"nextRound",finished+1));
            }
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
            if(Json.num(best,"from")!=id) {
                info("ELECT", "defer   best is N"+Json.num(best,"from"));
                return;
            }
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
            onlineByNode.clear(); heartbeatMisses.clear();
            followed=id;
            info("ELECT", "epoch "+term+"   votes "+votes+"/5   syncing logs");
            broadcast(Json.obj("type","LEADER_ANNOUNCE","from",id,"term",term));
            recover();
        } catch(Exception e) { info("WARN", "election failed   "+e.getClass().getSimpleName()); }
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
        if (!installQuorum(committed)) { info("WARN", "sync missed majority   writes blocked"); return; }
        ready=true;
        info("SYNC", "committed "+committed.size()+"   pending "+pending.size()+"   ready");
        for(LogStore.Entry e:pending) {
            if(log.byRequest(e.requestId())!=null) continue;
            Map<String,String> r=transact(e.op(),e.value(),e.requestId(),e.clientId(),e.time());
            if(!success(r)) { ready=false; info("WARN", "could not recover "+e.key()); return; }
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
                    Set<String> reported=ConcurrentHashMap.newKeySet();
                    for (String client:Json.get(response,"clients").split(",")) if(!client.isBlank()) reported.add(client);
                    onlineByNode.put(peer,reported);
                    heartbeatMisses.put(peer,0);
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
            } catch(Exception ignored) {
                int misses=heartbeatMisses.merge(peer,1,Integer::sum);
                if(misses>=3) onlineByNode.remove(peer);
            }
        }
        if(successes>=QUORUM) lastQuorum=System.currentTimeMillis();
        if(System.currentTimeMillis()-lastQuorum>TIMEOUT_MS && leaderId==id) {
            info("WARN", "lost quorum   stepping down"); leaderId=-1; ready=false; followed=-1;
            lastHeartbeat=System.currentTimeMillis()-TIMEOUT_MS;
            onlineByNode.clear();
        }
    }
    private static void writeRaw(BufferedWriter out, Map<String,String> message) throws IOException {
        out.write(Json.encode(message));
        out.write('\n');
        out.flush();
    }
    /** Node consoles may show the hidden number. Clients never receive it. */
    private void logTarget(String op, int value) {
        if ("WRITE".equals(op)) info("GAME", "initial target "+value);
        else if ("GUESS".equals(op))
            info("GAME", "round "+GameState.from(log.committed()).round()+" target "+value);
    }
    private static String opDetail(String op, int value, String client) {
        return switch(op) {
            case "JOIN" -> "JOIN  "+client;
            case "START" -> "START";
            case "STOP" -> "STOP  "+client;
            default -> op+"  "+value;
        };
    }
    private void logEvent(Map<String,String> event) {
        String type=Json.get(event,"type");
        String line=switch(type) {
            case "ROUND_RESULT" -> "round "+Json.get(event,"round")+"   winner "+Json.get(event,"winner");
            case "COUNTDOWN" -> Json.get(event,"seconds")+"s   next round "+Json.get(event,"nextRound");
            case "ROUND_OPEN" -> "round "+Json.get(event,"round")+" open   waiter "+Json.get(event,"waiter");
            case "GAME_RESET" -> "stopped   players and game state cleared";
            default -> Json.encode(event);
        };
        String tag=switch(type) {
            case "ROUND_RESULT" -> "WIN";
            case "COUNTDOWN" -> "WAIT";
            case "ROUND_OPEN","GAME_RESET" -> "GAME";
            default -> "PUSH";
        };
        info(tag, line);
    }
    private void pushEvent(Map<String,String> event) {
        logEvent(event);
        deliverLocal(event);
        Map<String,String> msg=new LinkedHashMap<>(event);
        msg.put("event",Json.get(event,"type"));
        msg.put("type","CLIENT_EVENT");
        msg.put("from",String.valueOf(id));
        msg.put("term",String.valueOf(term));
        broadcast(msg);
    }
    private void deliverLocal(Map<String,String> event) {
        for (ClientSession session:List.copyOf(sessions.values())) {
            try { session.send(event); }
            catch(IOException e) { dropSession(session); }
        }
    }
    private void attachSession(ClientSession session) {
        ClientSession previous=sessions.put(session.clientId,session);
        if (previous!=null && previous!=session) previous.close();
        info("ONLINE", session.clientId+" online   local");
        notifyPresence(session.clientId,true);
    }
    private void dropSession(ClientSession session) {
        if (session!=null && sessions.remove(session.clientId,session)) {
            info("ONLINE", session.clientId+" offline   local");
            notifyPresence(session.clientId,false);
        }
    }
    private void notifyPresence(String clientId, boolean online) {
        int leader=leaderId;
        if (leader<1 || leader==id) return;
        Map<String,String> msg=Json.obj("type","PRESENCE","from",id,"client_id",clientId,"online",online);
        workers.execute(()->{ try { Net.call(leader,msg,500); } catch(IOException ignored) {} });
    }
    public void start() throws Exception {
        ServerSocket listener=new ServerSocket(); listener.setReuseAddress(true);
        listener.bind(new InetSocketAddress("127.0.0.1",5000+id));
        info("LISTEN", "127.0.0.1:"+(5000+id)+"   term "+term);
        timer.scheduleWithFixedDelay(this::electionTick,300,300,TimeUnit.MILLISECONDS);
        timer.scheduleWithFixedDelay(()-> workers.execute(this::heartbeatTick),400,HEARTBEAT_MS,TimeUnit.MILLISECONDS);
        timer.scheduleWithFixedDelay(()->workers.execute(()->{try {gameTick();} catch(Exception e){info("WARN", "game tick   "+e.getClass().getSimpleName());}}),600,500,TimeUnit.MILLISECONDS);
        while(true) {
            Socket socket=listener.accept();
            workers.execute(()->{
                ClientSession session=null;
                try(socket;
                    BufferedReader r=new BufferedReader(new InputStreamReader(socket.getInputStream(),StandardCharsets.UTF_8));
                    BufferedWriter w=new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(),StandardCharsets.UTF_8))) {
                    socket.setSoTimeout(2500);
                    while(true) {
                        String line=r.readLine();
                        if(line==null || line.length()>2_000_000) break;
                        Map<String,String> request;
                        try { request=Json.decode(line); }
                        catch(Exception e) {
                            writeRaw(w,err("BAD_JSON"));
                            break;
                        }
                        String type=Json.get(request,"type");
                        boolean sessionMsg=isSessionMessage(type);
                        if(sessionMsg) socket.setSoTimeout(0);
                        Map<String,String> reply;
                        try { reply=dispatch(request); }
                        catch(Exception e) { reply=err("SERVER_ERROR_"+e.getClass().getSimpleName()); info("WARN", "request failed   "+e.getClass().getSimpleName()); }
                        String clientId=Json.get(request,"client_id");
                        if(sessionMsg && !type.equals("QUIT") && clientId.matches("[a-zA-Z0-9_-]{1,40}") && session==null) {
                            session=new ClientSession(clientId,socket,w);
                            attachSession(session);
                        }
                        if(session!=null) session.send(reply);
                        else writeRaw(w,reply);
                        if(!sessionMsg || type.equals("QUIT")) break;
                    }
                } catch(Exception ignored) {}
                finally { dropSession(session); }
            });
        }
    }
    public static void main(String[] args) throws Exception {
        if(args.length!=1) { System.out.println("Usage: java NodeServer <1..5>"); return; }
        new NodeServer(Integer.parseInt(args[0])).start();
    }
}
