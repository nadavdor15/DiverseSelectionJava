import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class EfficientUser extends EfficientDiverseSelector.TextArrayWritable {
    private String id;
    private int score;

    public EfficientUser() {
        super();
        this.id = "";
        this.score = 0;
    }

    public EfficientUser(String id, String[] strings) {
        super(strings);
        this.id = id;
        this.score = 0;
    }

    public EfficientUser(String id, int score, String[] strings) {
        super(strings);
        this.id = id;
        if (score < 0) {
            score = 0;
        }
        this.score = score;
    }

    public EfficientUser(String id, int score) {
        if (score < 0) {
            score = 0;
        }
        this.id = id;
        this.score = score;
    }

    private void calculateScore(Text[] groups) {
        if (this.score == 0) {
            int sum = 0;

            for (Text group : groups) {
                sum += Integer.parseInt(group.toString().split(",\\s+")[0]);
            }

            this.score = sum;
        }
    }

    public int getScore() {
        return this.score;
    }

    public String getId() {
        return this.id;
    }

    @Override
    public Text[] get() {
        Writable[] data = super.get();
        Text[] texts = new Text[data.length];
        for (int i = 0; i < texts.length; i++) {
            texts[i] = (Text) data[i];
        }
        return texts;
    }

    @Override
    public String toString() {
        Text[] data = this.get();

        if (data.length == 0) {
            return "";
        }

        this.calculateScore(data);

        return this.id + " " + this.score + " " + super.toString();
    }

    public EfficientUser clone() {
        EfficientUser duplicate = new EfficientUser(this.id, this.getScore());
        duplicate.set(this.get());
        return duplicate;
    }
}
