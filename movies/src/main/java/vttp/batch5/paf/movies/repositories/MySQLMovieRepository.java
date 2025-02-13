package vttp.batch5.paf.movies.repositories;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Repository;

import static vttp.batch5.paf.movies.utils.Queries.MYSQL_BATCH_INSERT;
import static vttp.batch5.paf.movies.utils.Queries.MYSQL_GET_MOVIE_BY_IMDB_ID;



@Repository
public class MySQLMovieRepository {

  @Autowired
  private JdbcTemplate sqlTemplate;

  // TODO: Task 2.3
  // You can add any number of parameters and return any type from the method
  public void batchInsertMovies(List<Object[]> params) throws DataAccessException{
    sqlTemplate.batchUpdate(MYSQL_BATCH_INSERT, params);
  }
  
  // TODO: Task 3
  public SqlRowSet getMovieDetails(String movieId){
    SqlRowSet rs = sqlTemplate.queryForRowSet(MYSQL_GET_MOVIE_BY_IMDB_ID, movieId);
    
    // moves cursor position 
    rs.next();

    return rs;
  }


}
