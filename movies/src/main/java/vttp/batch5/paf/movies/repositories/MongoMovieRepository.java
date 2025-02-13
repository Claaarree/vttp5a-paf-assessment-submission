package vttp.batch5.paf.movies.repositories;

import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

import static vttp.batch5.paf.movies.utils.Constants.MONGO_C_IMDB;
import static vttp.batch5.paf.movies.utils.Constants.MONGO_C_ERRORS;

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
 //


}
