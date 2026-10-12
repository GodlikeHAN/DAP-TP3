import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** One JSON line per short-lived TCP connection. */
public final class Net {
    private Net() {}
    static Map<String,String> call(int nodeId, Map<String,String> request, int timeoutMs) throws IOException {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("127.0.0.1", 5000+nodeId), Math.min(timeoutMs,400));
            s.setSoTimeout(timeoutMs);
            try (BufferedReader r=new BufferedReader(new InputStreamReader(s.getInputStream(),StandardCharsets.UTF_8));
                 BufferedWriter w=new BufferedWriter(new OutputStreamWriter(s.getOutputStream(),StandardCharsets.UTF_8))) {
                w.write(Json.encode(request)); w.write('\n'); w.flush();
                String line=r.readLine();
                if(line==null) throw new EOFException("Node closed connection");
                return Json.decode(line);
            }
        }
    }
}
