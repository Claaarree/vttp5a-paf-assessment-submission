package vttp.batch5.paf.movies.services;

import static vttp.batch5.paf.movies.utils.Constants.F_BUDGET;
import static vttp.batch5.paf.movies.utils.Constants.F_DIRECTORS;
import static vttp.batch5.paf.movies.utils.Constants.F_REVENUE;

import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;

@Service
public class MovieService {

  @Autowired
  private MongoMovieRepository mongoMovieRepository;

  @Autowired
  private MySQLMovieRepository mySQLMovieRepository;

  // TODO: Task 2
  

  // TODO: Task 3
  // You may change the signature of this method by passing any number of parameters
  // and returning any type
  public JsonArray getProlificDirectors(int limit) {
    List<Document> directorsList = mongoMovieRepository.getProlificDirectors(limit);

    JsonArrayBuilder jArrayBuilder = Json.createArrayBuilder();
    
    for(Document d: directorsList){
      Double totalRevenue = 0.0;
      Double totalBudget = 0.0;
      List<String> movieIds = d.getList("imdb_ids", String.class);
      for (String s : movieIds){
        SqlRowSet rs = mySQLMovieRepository.getMovieDetails(s);
        Double revenue = rs.getDouble(F_REVENUE);
        Double budget = rs.getDouble(F_BUDGET);

        totalRevenue += revenue;
        totalBudget += budget;
      }

      JsonObject jObject = Json.createObjectBuilder()
          .add("director_name", d.getString("_id"))
          .add("movies_count", d.getInteger("movies_count"))
          .add("total_revenue", totalRevenue)
          .add("total_budget", totalBudget)
          .build();

      jArrayBuilder.add(jObject);
    }

    return jArrayBuilder.build();
  }


  // TODO: Task 4
  // You may change the signature of this method by passing any number of parameters
  // and returning any type
  public void generatePDFReport() {

  }

}
