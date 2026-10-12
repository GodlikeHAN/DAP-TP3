import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/** Persistent JSON Lines transaction log. Only COMMITTED entries affect game state. */
public class LogStore {
    public record Entry(int epoch, int seq, String op, int value, String status,
                        String requestId, String clientId, long time) {
        String key() { return epoch + ":" + seq; }
        Map<String,String> json() {
            return Json.obj("epoch",epoch,"seq",seq,"op",op,"value",value,"status",status,
                    "request_id",requestId,"client_id",clientId,"time",time);
        }
        static Entry from(Map<String,String> m) {
            return new Entry(Json.num(m,"epoch"),Json.num(m,"seq"),Json.get(m,"op"),
                    Json.num(m,"value"),Json.get(m,"status"),Json.get(m,"request_id"),
                    Json.get(m,"client_id"),Json.longNum(m,"time"));
        }
        Entry withStatus(String s) { return new Entry(epoch,seq,op,value,s,requestId,clientId,time); }
    }
    private final Path file;
    private final List<Entry> records = new ArrayList<>();
    public LogStore(Path root, int id) throws IOException {
        Files.createDirectories(root);
        file = root.resolve("node-"+id+".jsonl");
        if (Files.exists(file)) for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) records.add(Entry.from(Json.decode(line)));
        }
    }
    synchronized List<Entry> all() { return new ArrayList<>(records); }
    synchronized List<Entry> committed() {
        return records.stream().filter(x -> x.status.equals("COMMITTED")).toList();
    }
    synchronized Entry byRequest(String req) {
        return records.stream().filter(e -> e.requestId.equals(req)).findFirst().orElse(null);
    }
    synchronized Entry byTag(int epoch, int seq) {
        return records.stream().filter(e -> e.epoch == epoch && e.seq == seq).findFirst().orElse(null);
    }
    synchronized Entry latestCommitted() {
        return committed().stream().max(Comparator.comparingInt(Entry::epoch).thenComparingInt(Entry::seq)).orElse(null);
    }
    synchronized void propose(Entry entry) throws IOException {
        Entry old = byTag(entry.epoch,entry.seq);
        if (old != null) {
            if (!old.op.equals(entry.op) || old.value != entry.value || !old.requestId.equals(entry.requestId))
                throw new IOException("Conflicting proposal tag");
            return;
        }
        records.add(entry);
        save();
    }
    synchronized boolean commit(int epoch, int seq) throws IOException {
        for (int i=0; i<records.size(); i++) if (records.get(i).epoch == epoch && records.get(i).seq == seq) {
            if (!records.get(i).status.equals("COMMITTED")) {
                records.set(i,records.get(i).withStatus("COMMITTED")); save();
            }
            return true;
        }
        return false;
    }
    synchronized void install(List<Entry> entries) throws IOException {
        records.clear(); records.addAll(entries); save();
    }
    private void save() throws IOException {
        StringBuilder b = new StringBuilder();
        for (Entry e : records) b.append(Json.encode(e.json())).append('\n');
        atomicWrite(file, b.toString());
    }
    static void atomicWrite(Path file, String data) throws IOException {
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING,StandardOpenOption.WRITE)) {
            ByteBuffer buf = StandardCharsets.UTF_8.encode(data);
            while (buf.hasRemaining()) ch.write(buf);
            ch.force(true);
        }
        try { Files.move(tmp,file,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING); }
        catch (AtomicMoveNotSupportedException e) { Files.move(tmp,file,StandardCopyOption.REPLACE_EXISTING); }
    }
    static String pack(List<Entry> records) {
        StringBuilder s = new StringBuilder();
        for (Entry e : records) s.append(Json.encode(e.json())).append('\n');
        return Base64.getEncoder().encodeToString(s.toString().getBytes(StandardCharsets.UTF_8));
    }
    static List<Entry> unpack(String b64) {
        String data = new String(Base64.getDecoder().decode(b64),StandardCharsets.UTF_8);
        List<Entry> list = new ArrayList<>();
        for (String line : data.split("\n")) if (!line.isBlank()) list.add(Entry.from(Json.decode(line)));
        return list;
    }
}
