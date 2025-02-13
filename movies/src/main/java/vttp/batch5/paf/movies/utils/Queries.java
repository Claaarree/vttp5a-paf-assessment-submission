package vttp.batch5.paf.movies.utils;

public class Queries {
    public static final String MYSQL_CHECK_DATA = "select * from imdb limit 1;";
    public static final String MYSQL_BATCH_INSERT = """
            insert into imdb(imdb_id, vote_average, vote_count, release_date, revenue, budget, runtime)
                    values(?, ?, ?, ?, ?, ?, ?)
            """;
        
    public static final String MYSQL_DELETE_BATCH = "delete from imdb where imdb_id = ?";

    public static final String MYSQL_GET_MOVIE_BY_IMDB_ID = "select * from imdb where imdb_id = ?;";
}
