package iust.recsys.bl;

import iust.recsys.da.LikeDAO;
import iust.recsys.da.MongoDbDAO;
import org.grouplens.lenskit.ItemRecommender;
import org.grouplens.lenskit.ItemScorer;
import org.grouplens.lenskit.RatingPredictor;
import org.grouplens.lenskit.RecommenderBuildException;
import org.grouplens.lenskit.baseline.BaselineScorer;
import org.grouplens.lenskit.baseline.ItemMeanRatingItemScorer;
import org.grouplens.lenskit.baseline.UserMeanBaseline;
import org.grouplens.lenskit.baseline.UserMeanItemScorer;
import org.grouplens.lenskit.core.LenskitConfiguration;
import org.grouplens.lenskit.core.LenskitRecommender;
import org.grouplens.lenskit.core.LenskitRecommenderEngine;
import org.grouplens.lenskit.data.dao.EventDAO;
import org.grouplens.lenskit.data.dao.ItemDAO;
import org.grouplens.lenskit.data.dao.UserDAO;
import org.grouplens.lenskit.data.dao.UserEventDAO;
import org.grouplens.lenskit.knn.item.ItemItemScorer;
import org.grouplens.lenskit.knn.user.UserUserItemScorer;
import org.grouplens.lenskit.scored.ScoredId;
import org.grouplens.lenskit.transform.normalize.BaselineSubtractingUserVectorNormalizer;
import org.grouplens.lenskit.transform.normalize.UserVectorNormalizer;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by meraj on 5/15/15.
 */
public class LensKitMongo {
    public LensKitMongo() throws RecommenderBuildException, IOException {
        LenskitConfiguration config = new LenskitConfiguration();
        // Use item-item CF to score items
        config.bind(ItemScorer.class)
                .to(UserUserItemScorer.class);
        // let's use personalized mean rating as the baseline/fallback predictor.
        // 2-step process:
        // First, use the user mean rating as the baseline scorer
        config.bind(BaselineScorer.class, ItemScorer.class)
                .to(UserMeanItemScorer.class);
        // Second, use the item mean rating as the base for user means
        config.bind(UserMeanBaseline.class, ItemScorer.class)
                .to(ItemMeanRatingItemScorer.class);
        // and normalize ratings by baseline prior to computing similarities
        config.bind(UserVectorNormalizer.class)
                .to(BaselineSubtractingUserVectorNormalizer.class);

        config.bind(EventDAO.class).to(MongoDbDAO.class);
//        config.bind(UserDAO.class).to(MongoDbDAO.class);
//        config.bind(ItemDAO.class).to(MongoDbDAO.class);
//        config.bind(UserEventDAO.class).to(MongoDbDAO.class);

//******************************
//        LenskitRecommender rec = LenskitRecommender.build(config);
//        ItemRecommender irec = rec.getItemRecommender();
//******************************

        LenskitRecommenderEngine engine = LenskitRecommenderEngine.newBuilder().addConfiguration(config).build();
//        LenskitRecommenderEngine engine = LenskitRecommenderEngine.load(new File("model.bin"));
        LenskitRecommender rec = engine.createRecommender();
        ItemRecommender irec = rec.getItemRecommender();

        engine.write(new File("model.bin"));

        List<ScoredId> recommendations = irec.recommend(33301824, 2);


        System.out.println(recommendations.size());
        for(ScoredId score : recommendations){
            System.out.println(score.getId());
        }
    }

    public static void main(String[] args) {
        try {
            LensKitMongo config = new LensKitMongo();
        } catch (RecommenderBuildException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
