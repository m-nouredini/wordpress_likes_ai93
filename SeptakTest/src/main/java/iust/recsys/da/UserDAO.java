package iust.recsys.da;

import org.grouplens.lenskit.cursors.Cursor;
import org.grouplens.lenskit.data.dao.EventDAO;
import org.grouplens.lenskit.data.dao.SortOrder;
import org.grouplens.lenskit.data.event.Event;

/**
 * Created by meraj on 5/16/15.
 */
public class UserDAO implements EventDAO {
    @Override
    public Cursor<Event> streamEvents() {
        return null;
    }

    @Override
    public <E extends Event> Cursor<E> streamEvents(Class<E> aClass) {
        return null;
    }

    @Override
    public <E extends Event> Cursor<E> streamEvents(Class<E> aClass, SortOrder sortOrder) {
        return null;
    }
}
