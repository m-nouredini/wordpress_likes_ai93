package iust.recsys.to;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.grouplens.lenskit.data.event.Event;
import org.grouplens.lenskit.data.history.AbstractUserHistory;
import org.grouplens.lenskit.data.history.UserHistory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by meraj on 5/16/15.
 */
public class UserLikeHistory<E extends Event> extends AbstractUserHistory<E> implements UserHistory<E>{
    private final long user;
    private final ImmutableList<E> events;

    public UserLikeHistory(long u, List<? extends E> es) {
        this.user = u;
        this.events = ImmutableList.copyOf(es);
    }


    @Override
    public long getUserId() {
        return user;
    }

    @Override
    public E get(int i) {
        return events.get(i);
    }

    @Override
    public int size() {
        return events.size();
    }

    @Override
    public Iterator<E> iterator() {
        return events.iterator();
    }

    @Override
    public List<E> subList(int from, int to) {
        return events.subList(from, to);
    }

    @Override
    public Object[] toArray() {
        return events.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return events.toArray(a);
    }


    @SuppressWarnings("unchecked")
    @Override
    public <T extends Event> UserHistory<T> filter(Class<T> type) {
        // pre-scan the history to see if we need to copy
        if (Iterables.all(this, Predicates.instanceOf(type))) {
            return (UserHistory<T>) this;
        } else {
            List<T> evts = ImmutableList.copyOf(Iterables.filter(this, type));
            return new UserLikeHistory<T>(getUserId(), evts);
        }
    }

    @Override
    public UserHistory<E> filter(Predicate<? super E> pred) {
        if (Iterables.all(this, pred)) {
            return this;
        } else {
            List<E> evts = ImmutableList.copyOf(Iterables.filter(this, pred));
            return new UserLikeHistory<E>(getUserId(), evts);
        }
    }

}
