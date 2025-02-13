package vttp.batch5.paf.movies.repositories;

import static vttp.batch5.paf.movies.utils.Constants.F_DIRECTORS;
import static vttp.batch5.paf.movies.utils.Constants.F_IMDB_ID;
import static vttp.batch5.paf.movies.utils.Constants.MONGO_C_IMDB;

import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.query.Criteria;

public class MongoRepo {

    @Autowired
    MongoTemplate mongoTemplate;
    
    public List<Document> haha() {
        Criteria criteria = Criteria.where(F_DIRECTORS)
                .ne("");
        MatchOperation removeEmpty = Aggregation.match(criteria);
        GroupOperation groupByDirector = Aggregation.group(F_DIRECTORS)
                .count().as("movies_count")
                .push(F_IMDB_ID).as("imdb_ids");

        SortOperation sortByMovies = Aggregation.sort(Direction.DESC, "movies_count");

        Aggregation pipeline = Aggregation.newAggregation(removeEmpty, groupByDirector, sortByMovies);
        return mongoTemplate.aggregate(pipeline, MONGO_C_IMDB, Document.class).getMappedResults();
    }
}
