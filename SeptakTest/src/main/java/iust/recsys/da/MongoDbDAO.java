package iust.recsys.da;

import com.mongodb.*;
import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongSet;
import iust.recsys.to.Like;
import iust.recsys.to.UserLikeHistory;
import org.grouplens.lenskit.cursors.Cursor;
import org.grouplens.lenskit.cursors.Cursors;
import org.grouplens.lenskit.data.dao.*;
import org.grouplens.lenskit.data.event.Event;
import org.grouplens.lenskit.data.history.ItemEventCollection;
import org.grouplens.lenskit.data.history.UserHistory;


import javax.annotation.Nullable;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by meraj on 5/16/15.
 */
public class MongoDbDAO implements EventDAO, UserEventDAO, ItemEventDAO, org.grouplens.lenskit.data.dao.UserDAO, ItemDAO {
    MongoClient mongoClient;
    DB db;
    DBCollection col;
    DBCursor cur;
    Iterator<DBObject> iterator;
    DBObject current;

    @Override
    public Cursor<Event> streamEvents() {
        DBObject obj = new BasicDBObject();
        obj.put("_id",0);
        obj.put("blog",1);
        obj.put("likes.dt",1);
        obj.put("likes.uid",1);
        try {
            mongoClient = new MongoClient( "localhost" );
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        db = mongoClient.getDB("test");
        col = db.getCollection("postslikes");
        iterator = col.find(null,obj).iterator();
        return Cursors.wrap(getEventIterator());
    }

    @Override
    public <E extends Event> Cursor<E> streamEvents(Class<E> aClass) {
        return (Cursor<E>) this.streamEvents();
    }

    @Override
    public <E extends Event> Cursor<E> streamEvents(Class<E> aClass, SortOrder sortOrder) {
        return (Cursor<E>) this.streamEvents();
    }

    @Override
    public LongSet getItemIds() {
        DBObject obj = new BasicDBObject();
        obj.put("_id",0);
        obj.put("blog",1);
        obj.put("likes",0);
        try {
            mongoClient = new MongoClient( "localhost" );
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        db = mongoClient.getDB("test");
        col = db.getCollection("postslikes");
        iterator = col.find(null,obj).iterator();
        LongSet set = new LongArraySet();
        while(iterator.hasNext()){
            set.add(Long.parseLong((String) iterator.next().get("blog")));
        }
        return set;
    }

    @Override
    public Cursor<ItemEventCollection<Event>> streamEventsByItem() {
        return null;
    }

    @Override
    public <E extends Event> Cursor<ItemEventCollection<E>> streamEventsByItem(Class<E> aClass) {
        return null;
    }

    @Override
    public List<Event> getEventsForItem(long l) {
        DBObject query = new BasicDBObject();
        query.put("blog",l);
        DBObject obj = new BasicDBObject();
        obj.put("_id",0);
        obj.put("blog",1);
        obj.put("likes.dt",1);
        obj.put("likes.uid",1);
        try {
            mongoClient = new MongoClient( "localhost" );
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        db = mongoClient.getDB("test");
        col = db.getCollection("postslikes");
        iterator = col.find(query,obj).iterator();
        return getEventList();
    }

    @Nullable
    @Override
    public <E extends Event> List<E> getEventsForItem(long l, Class<E> aClass) {
        DBObject query = new BasicDBObject();
        query.put("blog",l);
        DBObject obj = new BasicDBObject();
        obj.put("_id",0);
        obj.put("blog",1);
        obj.put("likes.dt",1);
        obj.put("likes.uid",1);
        try {
            mongoClient = new MongoClient( "localhost" );
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        db = mongoClient.getDB("test");
        col = db.getCollection("postslikes");
        iterator = col.find(query,obj).iterator();
        return (List<E>) getEventList();
    }

    @Nullable
    @Override
    public LongSet getUsersForItem(long l) {
        DBObject query = new BasicDBObject();
        query.put("blog",l);
        DBObject obj = new BasicDBObject();
        obj.put("_id",0);
        obj.put("blog",0);
        obj.put("likes.dt",0);
        obj.put("likes.uid",1);
        try {
            mongoClient = new MongoClient( "localhost" );
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        db = mongoClient.getDB("test");
        col = db.getCollection("postslikes");
        iterator = col.find(query,obj).iterator();
        LongSet set = new LongArraySet();
        while (iterator.hasNext()){
            set.add(Long.parseLong((String) ((DBObject)iterator.next().get("likes")).get("uid")));
        }
        return set;
    }

    @Override
    public LongSet getUserIds() {
        DBObject obj = new BasicDBObject();
        obj.put("_id",0);
        obj.put("blog",0);
        obj.put("likes.dt",0);
        obj.put("likes.uid",1);
        try {
            mongoClient = new MongoClient( "localhost" );
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        db = mongoClient.getDB("test");
        col = db.getCollection("postslikes");
        iterator = col.find(null,obj).iterator();
        LongSet set = new LongArraySet();
        while(iterator.hasNext()){
            set.add(Long.parseLong((String) ((DBObject) iterator.next().get("likes")).get("uid")));
        }
        return set;
    }

    @Override
    public Cursor<UserHistory<Event>> streamEventsByUser() {
        DBObject obj = new BasicDBObject();
        List<UserHistory<Event>> histories = new ArrayList<>();
        obj.put("likes",1);
        try {
            mongoClient = new MongoClient( "localhost" );
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        db = mongoClient.getDB("test");
        col = db.getCollection("trainUsers");
        iterator = col.find(null,obj).iterator();
        ArrayList<Event> likes = new ArrayList<>();
        long uid = 0l;
        while (iterator.hasNext()){
            DBObject object = iterator.next();
            uid = Long.parseLong((String) object.get("uid"));
            BasicDBList list = (BasicDBList) obj.get("likes");
            for(Object dbObject : list){
                DBObject o = (BasicDBObject) dbObject;
                Like like = new Like();
                like.setUserId(uid);
                like.setItemId(Long.parseLong((String) o.get("blog")));
                DateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss", Locale.ENGLISH);
                Date date = null;
                try {
                    date = format.parse((String) o.get("like_dt"));
                } catch (java.text.ParseException e) {
                    e.printStackTrace();
                }
                like.setTime(date.getTime());
                likes.add(like);
            }
            histories.add(new UserLikeHistory<Event>(uid,likes));
        }
        return Cursors.wrap(histories);

    }

    @Override
    public <E extends Event> Cursor<UserHistory<E>> streamEventsByUser(Class<E> aClass) {
        DBObject obj = new BasicDBObject();
        List<UserHistory<E>> histories = new ArrayList<>();
        obj.put("likes",1);
        try {
            mongoClient = new MongoClient( "localhost" );
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        db = mongoClient.getDB("test");
        col = db.getCollection("trainUsers");
        iterator = col.find(null,obj).iterator();
        ArrayList<E> likes = new ArrayList<>();
        long uid = 0l;
        while (iterator.hasNext()){
            DBObject object = iterator.next();
            uid = Long.parseLong((String) object.get("uid"));
            BasicDBList list = (BasicDBList) obj.get("likes");
            for(Object dbObject : list){
                DBObject o = (BasicDBObject) dbObject;
                Like like = new Like();
                like.setUserId(uid);
                like.setItemId(Long.parseLong((String) o.get("blog")));
                DateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss", Locale.ENGLISH);
                Date date = null;
                try {
                    date = format.parse((String) o.get("like_dt"));
                } catch (java.text.ParseException e) {
                    e.printStackTrace();
                }
                like.setTime(date.getTime());
                likes.add((E) like);
            }
            histories.add(new UserLikeHistory<E>(uid,likes));
        }
        return Cursors.wrap(histories);
    }

    @Nullable
    @Override
    public UserHistory<Event> getEventsForUser(long l) {
        DBObject query = new BasicDBObject();
        query.put("likes.uid",l);
        DBObject obj = new BasicDBObject();
        obj.put("_id",0);
        obj.put("blog",1);
        obj.put("likes.dt",1);
        obj.put("likes.uid",1);
        try {
            mongoClient = new MongoClient( "localhost" );
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        db = mongoClient.getDB("test");
        col = db.getCollection("postslikes");
        iterator = col.find(query,obj).iterator();
        return getUserHistory(l);
    }

    @Nullable
    @Override
    public <E extends Event> UserHistory<E> getEventsForUser(long l, Class<E> aClass) {
        DBObject query = new BasicDBObject();
        query.put("likes.uid",l);
        DBObject obj = new BasicDBObject();
        obj.put("_id",0);
        obj.put("blog",1);
        obj.put("likes.dt",1);
        obj.put("likes.uid",1);
        try {
            mongoClient = new MongoClient( "localhost" );
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        db = mongoClient.getDB("test");
        col = db.getCollection("postslikes");
        iterator = col.find(query,obj).iterator();
        return (UserHistory<E>) getUserHistory(l);
    }

    private Iterator<Event> getEventIterator(){
        List<Event> likes = new ArrayList<Event>();
        while (iterator.hasNext()){
            Like like = new Like();
            DBObject object = iterator.next();
            like.setItemId(Long.parseLong((String) object.get("blog")));
            DBObject likeObj = (DBObject) object.get("likes");
            like.setUserId(Long.parseLong((String) likeObj.get("uid")));
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss", Locale.ENGLISH);
            Date date = null;
            try {
                date = format.parse((String) likeObj.get("dt"));
            } catch (java.text.ParseException e) {
                e.printStackTrace();
            }
            like.setTime(date.getTime());
            likes.add(like);
        }
        return likes.iterator();
    }

    private List<Event> getEventList(){
        List<Event> likes = new ArrayList<Event>();
        while (iterator.hasNext()){
            Like like = new Like();
            DBObject object = iterator.next();
            like.setItemId(Long.parseLong((String) object.get("blog")));
            DBObject likeObj = (DBObject) object.get("likes");
            like.setUserId(Long.parseLong((String) likeObj.get("uid")));
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss", Locale.ENGLISH);
            Date date = null;
            try {
                date = format.parse((String) likeObj.get("dt"));
            } catch (java.text.ParseException e) {
                e.printStackTrace();
            }
            like.setTime(date.getTime());
            likes.add(like);
        }
        return likes;
    }

    private UserHistory<Event> getUserHistory(long l){
        ArrayList<Event> likes = new ArrayList<>();
        UserHistory<Event> history = null;
        while (iterator.hasNext()){
            Like like = new Like();
            DBObject object = iterator.next();
            like.setItemId(Long.parseLong((String) object.get("blog")));
            DBObject likeObj = (DBObject) object.get("likes");
            like.setUserId(Long.parseLong((String) likeObj.get("uid")));
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss", Locale.ENGLISH);
            Date date = null;
            try {
                date = format.parse((String) likeObj.get("dt"));
            } catch (java.text.ParseException e) {
                e.printStackTrace();
            }
            like.setTime(date.getTime());
            likes.add(like);
            history = new UserLikeHistory<Event>(l,likes);
        }
        return history;
    }
}
