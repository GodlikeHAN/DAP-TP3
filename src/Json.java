import java.util.LinkedHashMap;
import java.util.Map;

public final class Json {
    private Json() {}
    private static final java.util.Set<String> NUMBERS=java.util.Set.of(
            "epoch","seq","value","time","term","from","lastEpoch","lastSeq",
            "logSize","target","round","players","guesses","acks","leader","installed",
            "seconds","nextRound","countdown","online","need","lastValue");
    private static final java.util.Set<String> BOOLEANS=java.util.Set.of("ok","started","activeLeader","committed");
    public static Map<String, String> obj(Object... pairs) {
        if (pairs.length % 2 != 0) throw new IllegalArgumentException("pairs");
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) m.put(pairs[i].toString(), String.valueOf(pairs[i + 1]));
        return m;
    }
    public static String encode(Map<String,String> m) {
        StringBuilder b = new StringBuilder("{");
        for (var e : m.entrySet()) {
            if (b.length() > 1) b.append(',');
            b.append('"').append(escape(e.getKey())).append("\":");
            String key=e.getKey(), value=e.getValue();
            if ((NUMBERS.contains(key) && value.matches("-?[0-9]+")) ||
                (BOOLEANS.contains(key) && (value.equals("true") || value.equals("false"))))
                b.append(value);
            else b.append('"').append(escape(value)).append('"');
        }
        return b.append('}').toString();
    }
    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }
    public static Map<String,String> decode(String s) {
        Parser p = new Parser(s);
        return p.readObject();
    }
    private static final class Parser {
        private final String s; private int i;
        Parser(String s) { this.s = s; }
        void ws() { while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++; }
        void need(char c) { ws(); if (i >= s.length() || s.charAt(i++) != c) throw new IllegalArgumentException("Invalid JSON"); }
        String readString() {
            need('"'); StringBuilder b = new StringBuilder();
            while (i < s.length()) {
                char c = s.charAt(i++);
                if (c == '"') return b.toString();
                if (c != '\\') { b.append(c); continue; }
                if (i >= s.length()) throw new IllegalArgumentException("Invalid escape");
                char x = s.charAt(i++);
                switch (x) {
                    case '"' -> b.append('"'); case '\\' -> b.append('\\'); case '/' -> b.append('/');
                    case 'n' -> b.append('\n'); case 'r' -> b.append('\r'); case 't' -> b.append('\t');
                    case 'b' -> b.append('\b'); case 'f' -> b.append('\f');
                    case 'u' -> { if (i + 4 > s.length()) throw new IllegalArgumentException("Invalid unicode"); b.append((char)Integer.parseInt(s.substring(i, i+4), 16)); i += 4; }
                    default -> throw new IllegalArgumentException("Invalid escape");
                }
            }
            throw new IllegalArgumentException("Unterminated string");
        }
        Map<String,String> readObject() {
            need('{'); Map<String,String> m = new LinkedHashMap<>(); ws();
            if (i < s.length() && s.charAt(i) == '}') { i++; return m; }
            while (true) {
                String key = readString(); need(':'); ws();
                String value;
                if (s.charAt(i) == '"') value = readString();
                else {
                    int start = i;
                    while (i < s.length() && ",} \r\n\t".indexOf(s.charAt(i)) < 0) i++;
                    value = s.substring(start, i);
                    if (value.isEmpty() || value.equals("null")) throw new IllegalArgumentException("Invalid value");
                }
                m.put(key, value); ws();
                if (i < s.length() && s.charAt(i) == '}') { i++; break; }
                need(',');
            }
            ws(); if (i != s.length()) throw new IllegalArgumentException("Trailing JSON");
            return m;
        }
    }
    public static String get(Map<String,String> m, String k) { return m.getOrDefault(k, ""); }
    public static int num(Map<String,String> m, String k) { return Integer.parseInt(m.getOrDefault(k, "0")); }
    public static long longNum(Map<String,String> m, String k) { return Long.parseLong(m.getOrDefault(k, "0")); }
}
