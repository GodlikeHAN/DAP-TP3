import java.util.*;

/** Game state reconstructed from the COMMITTED log after crashes. */
public record GameState(List<String> members, boolean started, Integer target, int round, long lastJoinTime) {
    static GameState from(List<LogStore.Entry> log) {
        Set<String> members = new LinkedHashSet<>();
        boolean started = false; Integer target = null; int round=0; long lastJoin=0;
        for (LogStore.Entry e : log) {
            switch(e.op()) {
                case "JOIN" -> { members.add(e.clientId()); lastJoin=e.time(); }
                case "START" -> started=true;
                case "WRITE" -> { target=e.value(); round=1; }
                case "GUESS" -> { target=e.value(); round++; }
            }
        }
        return new GameState(new ArrayList<>(members),started,target,round,lastJoin);
    }
}
