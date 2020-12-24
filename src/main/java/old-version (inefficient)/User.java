import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class User extends ArrayWritable {
    private String id;
    private int score;

    public User() {
        super(DiverseSelector.TextArrayWritable.class);
        this.score = 0;
        this.id = "";
    }

    public User(String id, DiverseSelector.TextArrayWritable[] groups) {
        super(DiverseSelector.TextArrayWritable.class, groups);
        this.id = id;
        this.score = 0;
    }

    public User(String id, int score, DiverseSelector.TextArrayWritable[] groups) {
        super(DiverseSelector.TextArrayWritable.class, groups);

        if (score < 0) {
            score = 0;
        }
        this.id = id;
        this.score = score;
    }

    private void calculateScore(DiverseSelector.TextArrayWritable[] groups) {
        if (this.score == 0) {
            int sum = 0;

            for (DiverseSelector.TextArrayWritable group : groups) {
                sum += group.getSize();
            }

            this.score = sum;
        }
    }

    public int getScore() {
        return this.score;
    }

    @Override
    public DiverseSelector.TextArrayWritable[] get() {
        Writable[] data = super.get();
        DiverseSelector.TextArrayWritable[] textArrayWritables =
                new DiverseSelector.TextArrayWritable[super.get().length];
        for (int i = 0; i <textArrayWritables.length; i++) {
            textArrayWritables[i] = (DiverseSelector.TextArrayWritable) data[i];
        }
        return textArrayWritables;
    }

    @Override
    public String toString() {
        DiverseSelector.TextArrayWritable[] data = this.get();

        if (data.length == 0) {
            return "";
        }

        this.calculateScore(data);

        StringBuilder sb = new StringBuilder().append(this.id).append(" ").append(this.score).append(" ");
        for (DiverseSelector.TextArrayWritable w : data) {
            sb.append(w.toString()).append("$");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    public User clone() {
        DiverseSelector.TextArrayWritable[] textArrayWritables = this.get().clone();
        return new User(this.id, this.getScore(), textArrayWritables);
    }
}
