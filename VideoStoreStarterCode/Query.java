import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.io.FileInputStream;

/**
 * Runs queries against a back-end database
 */
public class Query {
  private static Properties configProps = new Properties();

  private static String imdbUrl;
  private static String customerUrl;

  private static String postgreSQLDriver;
  private static String postgreSQLUser;
  private static String postgreSQLPassword;

  // DB Connection
  private Connection _imdb;
  private Connection _customer_db;

  // Canned queries

  private String _search_sql = "SELECT * FROM movie WHERE name like ? ORDER BY id";
  private PreparedStatement _search_statement;

  private String _id_search_sql = "SELECT * FROM movie WHERE id = ?";
  private PreparedStatement _id_search_statement;

  private String _director_mid_sql = "SELECT y.* "
      + "FROM movie_directors x, directors y "
      + "WHERE x.mid = ? and x.did = y.id";
  private PreparedStatement _director_mid_statement;

  private String _casts_actor_mid_sql = "SELECT x.role, y.* " + "FROM casts x, actor y " + "WHERE x.mid = ? AND x.pid = y.id";
  private PreparedStatement _casts_actor_mid_statement;

  private String _rental_mid_sql = "SELECT x.cid " 
      + "FROM rental x " 
      + "WHERE x.mid = ? AND x.date_in IS NULL";
  private PreparedStatement _rental_mid_statement;

  private String _fs_directors_sql = "WITH movies as (" + _search_sql + ")" 
      + "SELECT movies.id, y.* " 
      + "FROM movies, movie_directors x, directors y " 
      + "WHERE x.mid = movies.id AND x.did = y.id "
      + "ORDER BY movies.id";
  private PreparedStatement _fs_directors_statement;

  private String _fs_actors_sql = "WITH movies as (" + _search_sql + ")" 
      + "SELECT movies.id, x.role, y.* "
      + "FROM movies, casts x, actor y "
      + "WHERE x.mid = movies.id AND x.pid = y.id "
      + "ORDER BY movies.id ";
  private PreparedStatement _fs_actors_statement;
  
  private String _movies_rented_sql = "SELECT x.*"
      + "FROM movie x, rental y "
      + "WHERE y.cid = ? AND y.mid = x.id AND y.date_in IS NULL";
  private PreparedStatement _movies_rented_statement;

  private String _customer_rentsleft_sql = "WITH movies_rented AS (" + _movies_rented_sql + ")"
      + "SELECT y.max_rentals - COUNT(movies_rented.*) "
      + "FROM Customer x, Rental_Plan y, cr "
      + "WHERE x.id = ? AND y.plan_name = x.plan ";
  private PreparedStatement _customer_rentsleft_statement;

  private String _customer_name_sql = "SELECT fname, lname "
      + "FROM Customer "
      + "WHERE id = ?";
  private PreparedStatement _customer_name_statement;
  
  private String _plans_sql = "SELECT * FROM Rental_Plan";
  private PreparedStatement _plans_statement;

  private String _customer_login_sql = "SELECT * FROM customer WHERE username = ? and password = ?";
  private PreparedStatement _customer_login_statement;

  private String _begin_transaction_read_write_sql = "BEGIN TRANSACTION READ WRITE";
  private PreparedStatement _begin_transaction_read_write_statement;

  private String _commit_transaction_sql = "COMMIT TRANSACTION";
  private PreparedStatement _commit_transaction_statement;

  private String _rollback_transaction_sql = "ROLLBACK TRANSACTION";
  private PreparedStatement _rollback_transaction_statement;

  public Query() {
  }

  /**********************************************************/
  /* Connections to postgres databases */

  public void openConnection() throws Exception {
    configProps.load(new FileInputStream("dbconn.config"));


    imdbUrl        = configProps.getProperty("imdbUrl");
    customerUrl    = configProps.getProperty("customerUrl");
    postgreSQLDriver   = configProps.getProperty("postgreSQLDriver");
    postgreSQLUser     = configProps.getProperty("postgreSQLUser");
    postgreSQLPassword = configProps.getProperty("postgreSQLPassword");


    /* load jdbc drivers */
    Class.forName(postgreSQLDriver).newInstance();

    /* open connections to TWO databases: imdb and the customer database */
    _imdb = DriverManager.getConnection(imdbUrl, // database
        postgreSQLUser, // user
        postgreSQLPassword); // password

    _customer_db = DriverManager.getConnection(customerUrl, // database
        postgreSQLUser, // user
        postgreSQLPassword); // password
  }

  public void closeConnection() throws Exception {
    _imdb.close();
    _customer_db.close();
  }

  /**********************************************************/
  /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

  public void prepareStatements() throws Exception {

    _search_statement = _imdb.prepareStatement(_search_sql);
    _id_search_statement = _imdb.prepareStatement(_id_search_sql);
    _director_mid_statement = _imdb.prepareStatement(_director_mid_sql);
    _casts_actor_mid_statement = _imdb.prepareStatement(_casts_actor_mid_sql);
    _rental_mid_statement = _imdb.prepareStatement(_rental_mid_sql);
    _fs_directors_statement = _imdb.prepareStatement(_fs_directors_sql);
    _fs_actors_statement = _imdb.prepareStatement(_fs_actors_sql);
    _customer_rentsleft_statement = _imdb.prepareStatement(_customer_rentsleft_sql);
    _customer_name_statement = _imdb.prepareStatement(_customer_name_sql);
    _movies_rented_statement = _imdb.prepareStatement(_movies_rented_sql);
    _plans_statement = _imdb.prepareStatement(_plans_sql);
    
    _customer_login_statement = _customer_db.prepareStatement(_customer_login_sql);
    _begin_transaction_read_write_statement = _customer_db.prepareStatement(_begin_transaction_read_write_sql);
    _commit_transaction_statement = _customer_db.prepareStatement(_commit_transaction_sql);
    _rollback_transaction_statement = _customer_db.prepareStatement(_rollback_transaction_sql);

    /* add here more prepare statements for all the other queries you need */
    /* . . . . . . */
  }


  /**********************************************************/
  /* suggested helper functions  */

  public int helper_compute_remaining_rentals(int cid) throws Exception {
    /* how many movies can she/he still rent ? */
    /* you have to compute and return the difference between the customer's plan
           and the count of oustanding rentals */
    _customer_rentsleft_statement.clearParameters();
    _customer_rentsleft_statement.setInt(1, cid);
    _customer_rentsleft_statement.setInt(2, cid);
    ResultSet _customer_rentsleft_set = _customer_rentsleft_statement.executeQuery();
    _customer_rentsleft_set.next();
    return _customer_rentsleft_set.getInt(1);
  }

  public String helper_compute_customer_name(int cid) throws Exception {
    /* you find  the first + last name of the current customer */
    _customer_name_statement.clearParameters();
    _customer_name_statement.setInt(1, cid);
    ResultSet _customer_name_set = _customer_name_statement.executeQuery();
    _customer_name_set.next();
    return _customer_name_set.getString(1) + " " + _customer_name_set.getString(2);
  }

  public boolean helper_check_plan(int plan_id) throws Exception {
    /* is plan_id a valid plan id ?  you have to figure out */
    return true;
  }

  public boolean helper_check_movie(int mid) throws Exception {
    /* is mid a valid movie id ? you have to figure out  */
    _id_search_statement.clearParameters();
    _id_search_statement.setInt(1, mid);
    ResultSet _id_search_set = _id_search_statement.executeQuery();
    while (_id_search_set.next()) {
      return true;
    }
    return false;
  }

  private int helper_who_has_this_movie(int mid) throws Exception {
    /* find the customer id (cid) of whoever currently rents the movie mid; return -1 if none */
    // TODO check for valid mid?  
    _rental_mid_statement.clearParameters();
    _rental_mid_statement.setInt(1, mid);
    ResultSet rental_set = _rental_mid_statement.executeQuery();
    while (rental_set.next()) {
      rental_set.close();
      return rental_set.getInt(1);
    }
    rental_set.close();
    return -1;
  }

  /**********************************************************/
  /* login transaction: invoked only once, when the app is started  */
  public int transaction_login(String name, String password) throws Exception {
    /* authenticates the user, and returns the user id, or -1 if authentication fails */

    int cid;

    _customer_login_statement.clearParameters();
    _customer_login_statement.setString(1,name);
    _customer_login_statement.setString(2,password);
    ResultSet cid_set = _customer_login_statement.executeQuery();
    if (cid_set.next()) cid = cid_set.getInt(1);
    else cid = -1;
    return(cid);
  }

  public void transaction_personal_data(int cid) throws Exception {
    /* println the customer's personal data: name, and plan number */
  }


  /**********************************************************/
  /* main functions in this project: */

  // 1:38
  public void transaction_search(int cid, String movie_title)
      throws Exception {
    /* searches for movies with matching titles: SELECT * FROM movie WHERE name LIKE movie_title */
    /* prints the movies, directors, actors, and the availability status:
           AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT */

    /* set the first (and single) '?' parameter */
    _search_statement.clearParameters();
    _search_statement.setString(1, '%' + movie_title + '%');

    ResultSet movie_set = _search_statement.executeQuery();
    while (movie_set.next()) {
      int mid = movie_set.getInt(1);
      System.out.println("ID: " + mid + " NAME: "
          + movie_set.getString(2) + " YEAR: "
          + movie_set.getString(3));
      /* do a dependent join with directors */
      _director_mid_statement.clearParameters();
      _director_mid_statement.setInt(1, mid);
      ResultSet director_set = _director_mid_statement.executeQuery();
      while (director_set.next()) {
        System.out.println("\t\tDIRECTOR: " + director_set.getString(3)
        + " " + director_set.getString(2));
      }
      director_set.close();
      /* now you need to retrieve the actors, in the same manner */
      _casts_actor_mid_statement.clearParameters();
      _casts_actor_mid_statement.setInt(1, mid);
      ResultSet casts_actor_set = _casts_actor_mid_statement.executeQuery();
      while (casts_actor_set.next()) {
        System.out.println("\t\tACTOR: " + casts_actor_set.getString(3) 
        + " " + casts_actor_set.getString(4) + " | ROLE: " + casts_actor_set.getString(1));
      }
      casts_actor_set.close();
      /* then you have to find the status: of "AVAILABLE" "YOU HAVE IT", "UNAVAILABLE" */
      int renterID = this.helper_who_has_this_movie(mid);
      System.out.print("\t\tMOVIE STATUS: ");
      if (cid == renterID) System.out.println("YOU HAVE IT");
      else if (renterID < 0) System.out.println("AVAILABLE");
      else System.out.println("UNAVAILABLE");
    }
    System.out.println();
  }

  public void transaction_choose_plan(int cid, int pid) throws Exception {
    /* updates the customer's plan to pid: UPDATE customers SET plid = pid */
    /* remember to enforce consistency ! */
    if (helper_check_plan(pid)) {
      // TODO change plan transaction
    } else {
      System.out.println("Invalid plan.\n");
    }
  }

  public void transaction_list_plans() throws Exception {
    /* println all available plans: SELECT * FROM plan */
    _plans_statement.clearParameters();
    ResultSet _plans_set = _plans_statement.executeQuery();
    System.out.println("PLANS AVAILABLE: ");
    while (_plans_set.next()) {
      System.out.println("\tPLAN: " + _plans_set.getString(1) 
      + " | MONTHLY FEE: " + _plans_set.getString(2)
      + " | MAX RENTALS: " + _plans_set.getString(3));
    }
    System.out.println();
  }

  public void transaction_list_user_rentals(int cid) throws Exception {
    /* println all movies rented by the current user*/
    _movies_rented_statement.clearParameters();
    _movies_rented_statement.setInt(1, cid);
    ResultSet _movies_rented_set = _movies_rented_statement.executeQuery();
    System.out.println("YOUR RENTED MOVIES:");
    while (_movies_rented_set.next()) {
      System.out.println("\t\tMOVIE NAME:" + _movies_rented_set.getString(2) + " | YEAR: " 
          + _movies_rented_set.getInt(3));
    }
    System.out.println();
  }

  public void transaction_rent(int cid, int mid) throws Exception {
    /* rent the movie mid to the customer cid */
    /* remember to enforce consistency ! */
  }

  public void transaction_return(int cid, int mid) throws Exception {
    /* return the movie mid by the customer cid */
  }

  // 3:48
  public void transaction_fast_search(int cid, String movie_title)
      throws Exception {
    /* like transaction_search, but uses joins instead of independent joins
           Needs to run three SQL queries: (a) movies, (b) movies join directors, (c) movies join actors
           Answers are sorted by mid.
           Then merge-joins the three answer sets */
    _search_statement.clearParameters();
    _search_statement.setString(1, '%' + movie_title + '%');
    ResultSet movie_set = _search_statement.executeQuery();
    _fs_directors_statement.clearParameters();
    _fs_directors_statement.setString(1, '%' + movie_title + '%');
    ResultSet _fs_director_set = _fs_directors_statement.executeQuery();
    _fs_actors_statement.clearParameters();
    _fs_actors_statement.setString(1, '%' + movie_title + '%');
    ResultSet _fs_actor_set = _fs_actors_statement.executeQuery();
    /*
    while (movie_set.next()) {
      int mid = movie_set.getInt(1);
      System.out.println("ID: " + mid + " NAME: "
          + movie_set.getString(2) + " YEAR: "
          + movie_set.getString(3));
      if (_fs_director_set.getInt(1) == mid) {
        System.out.println("\t\tDIRECTOR: " + _fs_director_set.getString(4)
        + " " + _fs_director_set.getString(3));
      } else {
        _fs_director_set.next();
      }
      if (_fs_actor_set.getInt(1) == mid) {
        System.out.println("\t\tACTOR: " + _fs_actor_set.getString(4) 
        + " " + _fs_actor_set.getString(5) + " | ROLE: " + _fs_actor_set.getString(2));
      } else {
        _fs_actor_set.next();
      }
      System.out.println();
    }
    */
    _fs_director_set.close();
    _fs_actor_set.close();
    movie_set.close();
  }

}
