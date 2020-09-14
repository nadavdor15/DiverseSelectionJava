import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class User extends ArrayWritable {
    private int score;

    public User() {
        super(DiverseSelector.TextArrayWritable.class);
        this.score = 0;
    }

    public User(DiverseSelector.TextArrayWritable[] groups) {
        super(DiverseSelector.TextArrayWritable.class);
        super.set(groups);
    }

    private void calculateScore(DiverseSelector.TextArrayWritable[] groups) {
        int sum = 0;

        for (DiverseSelector.TextArrayWritable group : groups) {
            sum += group.getSize();
        }

        this.score = sum;
    }

    @Override
    public String toString() {
        Writable[] data = super.get();

        if (data.length == 0) {
            return "";
        }

        this.calculateScore((DiverseSelector.TextArrayWritable[]) data);

        StringBuilder sb = new StringBuilder().append(this.score).append(" ");
        for (Writable w : data) {
            sb.append(w.toString()).append("$");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }
}
