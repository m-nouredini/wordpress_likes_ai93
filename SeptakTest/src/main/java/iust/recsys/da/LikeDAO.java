package iust.recsys.da;

import org.grouplens.lenskit.cursors.Cursor;
import org.grouplens.lenskit.data.dao.EventDAO;
import org.grouplens.lenskit.data.dao.SortOrder;
import org.grouplens.lenskit.data.event.Event;

import java.io.IOException;

/**
 * Created by meraj on 5/15/15.
 */
public class LikeDAO implements EventDAO {
    @Override
    public Cursor<Event> streamEvents() {
        Cursor<Event> cur = null;
        try {
            cur = new LikeCursorImpl();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cur;
    }

    @Override
    public <E extends Event> Cursor<E> streamEvents(Class<E> aClass) {
        Cursor<E> cur = null;
        try {
            cur = (Cursor<E>) new LikeCursorImpl();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cur;
    }

    @Override
    public <E extends Event> Cursor<E> streamEvents(Class<E> aClass, SortOrder sortOrder) {
        Cursor<E> cur = null;
        try {
            cur = (Cursor<E>) new LikeCursorImpl();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cur;
    }
//
//    public static void main(String[] args) {
//        LikeDAO dao = new LikeDAO();
//        Cursor<Event> cur = dao.streamEvents();
//        for(Event e : cur){
//            System.out.println(e.getUserId());
//        }
//    }

}
