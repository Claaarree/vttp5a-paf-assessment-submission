package vttp.batch5.paf.movies.bootstrap;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;

import static vttp.batch5.paf.movies.utils.Constants.F_BUDGET;
import static vttp.batch5.paf.movies.utils.Constants.F_DIRECTORS;
import static vttp.batch5.paf.movies.utils.Constants.F_GENRES;
import static vttp.batch5.paf.movies.utils.Constants.F_IMDB_ID;
import static vttp.batch5.paf.movies.utils.Constants.F_IMDB_RATING;
import static vttp.batch5.paf.movies.utils.Constants.F_IMDB_VOTES;
import static vttp.batch5.paf.movies.utils.Constants.F_OVERVIEW;
import static vttp.batch5.paf.movies.utils.Constants.F_RELEASE_DATE;
import static vttp.batch5.paf.movies.utils.Constants.F_REVENUE;
import static vttp.batch5.paf.movies.utils.Constants.F_RUNTIME;
import static vttp.batch5.paf.movies.utils.Constants.F_TAGLINE;
import static vttp.batch5.paf.movies.utils.Constants.F_TITLE;
import static vttp.batch5.paf.movies.utils.Constants.F_VOTE_AVERAGE;
import static vttp.batch5.paf.movies.utils.Constants.F_VOTE_COUNT;
import static vttp.batch5.paf.movies.utils.Constants.MONGO_C_IMDB;
import static vttp.batch5.paf.movies.utils.Queries.MYSQL_CHECK_DATA;
import static vttp.batch5.paf.movies.utils.Queries.MYSQL_DELETE_BATCH;

@Component
public class Dataloader implements CommandLineRunner{

  @Autowired
  private MongoTemplate mongoTemplate;

  @Autowired
  private JdbcTemplate sqlTemplate;

  @Autowired
  private MongoMovieRepository mongoMovieRepository;

  @Autowired
  private MySQLMovieRepository sqlMovieRepository;

  //TODO: Task 2
  @Override
  public void run(String... args) throws Exception {
    System.out.println("in data Loader");

    // checks if there is data in the db
    Long mongoCount = mongoTemplate.estimatedCount(MONGO_C_IMDB);
    SqlRowSet rs = sqlTemplate.queryForRowSet(MYSQL_CHECK_DATA);
    Boolean sqlDataPresent = rs.next();

    int lineCount = 0;

    // load data if none
    if (mongoCount == 0 || !sqlDataPresent){
      File f = null;
      if (args.length != 1) {
        f = new File("../data/movies_post_2010.zip");
      } else {
        f = new File(args[0]);
      }

      ZipFile zipFile = new ZipFile(f);
      ZipEntry zipEntry = zipFile.getEntry("movies_post_2010.json");
      InputStream is = zipFile.getInputStream(zipEntry);
      InputStreamReader isr = new InputStreamReader(is);
      BufferedReader br = new BufferedReader(isr);


      List<Document> rawDocs = new ArrayList<>();

      String line = "";
      while ((line = br.readLine()) != null){
        lineCount++;
        // check release_date of movies
        Document d = Document.parse(line);
        String dateString = d.getString(F_RELEASE_DATE);
        LocalDate ld = LocalDate.parse(dateString);
        LocalDate cutOff = LocalDate.of(2018, 1, 1);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = sdf.parse(dateString);
        
        // remove any movies released before 2018
        if(ld.isAfter(cutOff)){
          rawDocs.add(d);
          
          if(rawDocs.size() == 25 || line == null){
            if (!sqlDataPresent){
              List<Object[]> params = rawDocs.stream()
              .map(document -> new Object[]{
                document.getOrDefault(F_IMDB_ID, ""), 
                document.getOrDefault(F_VOTE_AVERAGE, 0),
                document.getOrDefault(F_VOTE_COUNT, 0),
                date,
                document.getOrDefault(F_REVENUE, 0),
                document.getOrDefault(F_BUDGET, 0),
                document.getOrDefault(F_RUNTIME, 0)})
              .collect(Collectors.toList());

              try {
                sqlMovieRepository.batchInsertMovies(params);

                // only if mysql update is successful, update mongodb
                if(mongoCount == 0) {
                  List<Document> toInsert = new ArrayList<>();
                  for (Document rd : rawDocs){
                    Document doc = new Document();
                    doc.append(F_IMDB_ID, rd.getOrDefault(F_IMDB_ID,""));
                    doc.append(F_TITLE, rd.getOrDefault(F_TITLE, ""));
                    doc.append(F_DIRECTORS, rd.getOrDefault("director", ""));
                    doc.append(F_OVERVIEW, rd.getOrDefault(F_OVERVIEW, ""));
                    doc.append(F_TAGLINE, rd.getOrDefault(F_TAGLINE, ""));
                    doc.append(F_GENRES, rd.getOrDefault(F_GENRES, ""));
                    doc.append(F_IMDB_RATING, rd.getOrDefault(F_IMDB_RATING, 0));
                    doc.append(F_IMDB_VOTES, rd.getOrDefault(F_IMDB_VOTES, 0));
                    toInsert.add(doc);
                  }
                  try {
                    mongoMovieRepository.batchInsertMovies(toInsert);
                  } catch (Exception ex) {
                    List<Object[]> failedID = rawDocs.stream()
                      .map(document -> new Object[]{
                        document.getOrDefault(F_IMDB_ID, "")})
                      .collect(Collectors.toList());
                    sqlTemplate.batchUpdate(MYSQL_DELETE_BATCH, failedID);
                    throw ex;
                  }
                }

              } catch (Exception e) {
                String[] ids = new String[rawDocs.size()];
                for (int i = 0; i < ids.length; i++){
                  ids[i] = rawDocs.get(i).getString(F_IMDB_ID);
                }
                Document err = new Document();
                err.append("imdb_ids", ids);
                err.append("error", e.getMessage());
                err.append("timestamp", new Date());

                mongoMovieRepository.logError(err);
              }
      
            } 
            rawDocs = new ArrayList<>();
            System.out.println("starting new batch");
          }
        }
        
        
        
      }    

      zipFile.close();
    }
    System.out.println(">>>" + lineCount);
  }
    



}
