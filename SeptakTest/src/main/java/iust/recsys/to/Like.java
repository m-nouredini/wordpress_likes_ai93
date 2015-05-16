package iust.recsys.to;

import org.grouplens.lenskit.data.dao.EventDAO;
import org.grouplens.lenskit.data.event.Event;
import org.grouplens.lenskit.data.event.Rating;
import org.grouplens.lenskit.data.pref.Preference;

import javax.annotation.Nullable;
import java.util.Random;

/**
 * Created by meraj on 5/15/15.
 */
public class Like implements Event, Rating {
    private long userId;
    private long itemId;
    private long time;
    private LikePref pref;

    public Like(){
        pref = new LikePref();
    }

    public void setUserId(long userId) {
        this.userId = userId;
        pref.setUserId(userId);
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
        pref.setItemId(itemId);
        Random rand = new Random();
        Integer randomNum = rand.nextInt((5 - 0) + 1) + 0;
        pref.setVal(randomNum);
    }

    public void setTime(long time) {
        this.time = time;

    }

    @Override
    public long getUserId() {
        return userId;
    }

    @Override
    public long getItemId() {
        return itemId;
    }

    @Override
    public long getTimestamp() {
        return time;
    }


    @Nullable
    @Override
    public Preference getPreference() {
        return pref;
    }

    @Override
    public boolean hasValue() {
        return true;
    }

    @Override
    public double getValue() throws IllegalStateException {
        return pref.getValue();
    }

    private class LikePref implements Preference{
        private long userId;
        private long itemId;
        private double val;

        public void setVal(double val) {
            this.val = val;
        }

        public void setUserId(long userId) {
            this.userId = userId;
        }

        public void setItemId(long itemId) {
            this.itemId = itemId;
        }

        @Override
        public long getUserId() {
            return userId;
        }

        @Override
        public long getItemId() {
            return itemId;
        }

        @Override
        public double getValue() {
            return val;
        }
    }
}
