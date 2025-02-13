package vttp.batch5.paf.movies.services;

import static vttp.batch5.paf.movies.utils.Constants.F_BUDGET;
import static vttp.batch5.paf.movies.utils.Constants.F_REVENUE;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.JasperCompileManager;
import net.sf.jasperreports.engine.JasperExportManager;
import net.sf.jasperreports.engine.JasperFillManager;
import net.sf.jasperreports.engine.JasperPrint;
import net.sf.jasperreports.engine.JasperReport;
import net.sf.jasperreports.json.data.JsonDataSource;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;

@Service
public class MovieService {

  @Autowired
  private MongoMovieRepository mongoMovieRepository;

  @Autowired
  private MySQLMovieRepository mySQLMovieRepository;

  @Value("${jasondatasource.name}")
  String name;

  @Value("${jsondatasource.batch}")
  String batch;


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
  public void generatePDFReport(int count) {
   
    JsonObject jObject1 = Json.createObjectBuilder()
        .add("name", name)
        .add("batch", batch)
        .build();
    
    InputStream is1 = new ByteArrayInputStream(jObject1.toString().getBytes());
    JsonArray jArray = getProlificDirectors(count);
    JsonArrayBuilder jArrayBuilderNew = Json.createArrayBuilder();

    for (int i = 0; i < jArray.size(); i++){
      JsonObject jObject = jArray.getJsonObject(i);
      JsonObject newObject = Json.createObjectBuilder()
          .add("director", jObject.getString("director_name"))
          .add("count", jObject.getInt("movies_count"))
          .add("revenue", jObject.getJsonNumber("total_revenue"))
          .add("budget", jObject.getJsonNumber("total_budget"))
          .build();
      jArrayBuilderNew.add(newObject);
    }
    InputStream is2 = new ByteArrayInputStream(jArrayBuilderNew.build().toString().getBytes());

    try {
      JsonDataSource reportDS = new JsonDataSource(is1);
      JsonDataSource directorsDS = new JsonDataSource(is2);

      Map<String, Object> params = new HashMap<>();
      params.put("DIRECTOR_TABLE_DATASET", directorsDS);

      JasperReport report = JasperCompileManager.compileReport("..\\data\\director_movies_report.jrxml");

      JasperPrint print = JasperFillManager.fillReport(report, params, reportDS);

      JasperExportManager.exportReportToPdfFile(print, "..\\data\\director_movies_report.pdf");

    } catch (JRException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
