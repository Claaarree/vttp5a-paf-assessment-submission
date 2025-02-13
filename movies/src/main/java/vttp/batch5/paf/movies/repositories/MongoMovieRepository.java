package vttp.batch5.paf.movies.repositories;

import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.LimitOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Repository;

import static vttp.batch5.paf.movies.utils.Constants.MONGO_C_IMDB;
import static vttp.batch5.paf.movies.utils.Constants.MONGO_C_ERRORS;
import static vttp.batch5.paf.movies.utils.Constants.F_DIRECTORS;
import static vttp.batch5.paf.movies.utils.Constants.F_IMDB_ID;;


@Repository
public class MongoMovieRepository {

    @Autowired
    private MongoTemplate mongoTemplate;


 // TODO: Task 2.3
 // You can add any number of parameters and return any type from the method
 // You can throw any checked exceptions from the method
 // Write the native Mongo query you implement in the method in the comments
 //
 //    native MongoDB query here
  //  db.imdb.insertMany(
  // 	{<field1>: <value>, <field2>: <value>,...},
  // 	{<field1>: <value>, <field2>: <value>,...},
  //  {...}
  // )
 //
 public void batchInsertMovies(List<Document> toInsert) {
  mongoTemplate.insert(toInsert, MONGO_C_IMDB);

 }

 // TODO: Task 2.4
 // You can add any number of parameters and return any type from the method
 // You can throw any checked exceptions from the method
 // Write the native Mongo query you implement in the method in the comments
 //
 //    native MongoDB query here
    //  db.errors.insert(
    // 	{imdb_ids: ["id1", "id2", ...], 
    // error: exception.getMessage(), 
    // timestamp: "date when exception occurred"}
    // )
 //
 public void logError(Document err) {
    mongoTemplate.insert(err, MONGO_C_ERRORS);

 }

 // TODO: Task 3
 // Write the native Mongo query you implement in the method in the comments
 //
 //    native MongoDB query here
//  db.imdb.aggregate([
//     {$match: {directors: {$ne: ""}}},
//     {$group: {
//         _id: '$directors',
//         movies_count: {'$sum': 1},
//         imdb_ids: {$push: '$imdb_id'}
//     }},
//     {$sort: {movies_count: -1, _id: 1}},
//     {$limit: 5}
// ])
 //
 public List<Document> getProlificDirectors(int limit) {
    Criteria criteria = Criteria.where(F_DIRECTORS)
            .ne("");
    MatchOperation removeEmpty = Aggregation.match(criteria);
    GroupOperation groupByDirector = Aggregation.group(F_DIRECTORS)
            .count().as("movies_count")
            .push(F_IMDB_ID).as("imdb_ids");

    SortOperation sortByMovies = Aggregation.sort(Direction.DESC, "movies_count")
            .and(Direction.ASC, "_id");
            
    LimitOperation limitOps = Aggregation.limit(limit); 

    Aggregation pipeline = Aggregation.newAggregation(removeEmpty, groupByDirector, sortByMovies, limitOps);
    return mongoTemplate.aggregate(pipeline, MONGO_C_IMDB, Document.class).getMappedResults();
 }


}
