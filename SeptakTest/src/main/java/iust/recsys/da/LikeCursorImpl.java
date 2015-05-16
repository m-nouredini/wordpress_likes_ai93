package iust.recsys.da;

import com.mongodb.*;
import iust.recsys.to.Like;
import org.grouplens.lenskit.cursors.Cursor;
import org.grouplens.lenskit.data.event.Event;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by meraj on 5/15/15.
 */
public class LikeCursorImpl implements Cursor<Event> {
    MongoClient mongoClient;
    DB db;
    DBCollection col;
    DBCursor cur;
    Iterator<DBObject> iterator;
    DBObject current;


    public LikeCursorImpl() throws IOException {
//        mongoClient = new MongoClient( "localhost" );
//        db = mongoClient.getDB("test");
//        col = db.getCollection("testCol");
//        BasicDBObject fields = new BasicDBObject();
//        fields.put("blog", 1);
//        fields.put("likes", 1);
//        List<DBObject> pipeline = new ArrayList<DBObject>();
//        pipeline.add(new BasicDBObject("$unwind", "$likes"));
//        DBObject obj = new BasicDBObject();
//        obj.put("_id",0);
//        obj.put("blog",1);
//        obj.put("likes.dt",1);
//        obj.put("likes.uid",1);
//        pipeline.add(new BasicDBObject("$project", obj));
//        pipeline.add(new BasicDBObject("$out", "postslikes"));
//        AggregationOutput output = col.aggregate(pipeline);
//        iterator = output.results().iterator();

        DBObject obj = new BasicDBObject();
        obj.put("_id",0);
        obj.put("blog",1);
        obj.put("likes.dt",1);
        obj.put("likes.uid",1);
        mongoClient = new MongoClient( "localhost" );
        db = mongoClient.getDB("test");
        col = db.getCollection("postslikes");
        iterator = col.find(null,obj).iterator();

    }

    @Override
    public int getRowCount() {
        return 0;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Nonnull
    @Override
    public Like next() {
        Like like = new Like();
        DBObject object = iterator.next();
        like.setItemId(Long.parseLong((String) object.get("blog")));
        DBObject likeObj = (DBObject) object.get("likes");
        like.setUserId(Long.parseLong((String) likeObj.get("uid")));
//            "2012-04-01 08:17:25"
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss", Locale.ENGLISH);
        Date date = null;
        try {
            date = format.parse((String) likeObj.get("dt"));
        } catch (java.text.ParseException e) {
            e.printStackTrace();
        }
        like.setTime(date.getTime());
        return like;
    }

    @Nonnull
    @Override
    public Like fastNext() {
        Like like = new Like();
        DBObject object = iterator.next();
        like.setItemId(Long.parseLong((String) object.get("blog")));
        DBObject likeObj = (DBObject) object.get("likes");
        like.setUserId(Long.parseLong((String) likeObj.get("uid")));
//            "2012-04-01 08:17:25"
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss", Locale.ENGLISH);
        Date date = null;
        try {
            date = format.parse((String) likeObj.get("dt"));
        } catch (java.text.ParseException e) {
            e.printStackTrace();
        }
        like.setTime(date.getTime());
        return like;
    }

    @Override
    public Iterable<Event> fast() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public Iterator<Event> iterator() {
        List<Event> likes = new ArrayList<Event>();
        while (iterator.hasNext()){
            Like like = new Like();
            DBObject object = iterator.next();
            like.setItemId(Long.parseLong((String) object.get("blog")));
            DBObject likeObj = (DBObject) object.get("likes");
            like.setUserId(Long.parseLong((String) likeObj.get("uid")));
//            "2012-04-01 08:17:25"
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
}
//public class CrunchifyJSONReadFromFile {
//
//    @SuppressWarnings("unchecked")
//    public static void main(String[] args) {
//        JSONParser parser = new JSONParser();
//
//        try {
//
//            Object obj = parser.parse(new FileReader(
//                    "/Users/<username>/Documents/file1.txt"));
//
//            JSONObject jsonObject = (JSONObject) obj;
//
//            String name = (String) jsonObject.get("Name");
//            String author = (String) jsonObject.get("Author");
//            JSONArray companyList = (JSONArray) jsonObject.get("Company List");
//
//            System.out.println("Name: " + name);
//            System.out.println("Author: " + author);
//            System.out.println("\nCompany List:");
//            Iterator<String> iterator = companyList.iterator();
//            while (iterator.hasNext()) {
//                System.out.println(iterator.next());
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}