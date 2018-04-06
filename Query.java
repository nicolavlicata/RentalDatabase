import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.*;
import java.text.SimpleDateFormat;

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

    private String _director_mid_sql = "SELECT y.* "
                     + "FROM movie_directors x, directors y "
                     + "WHERE x.mid = ? and x.did = y.id";
    private PreparedStatement _director_mid_statement;

    private String _count_rentals_sql = "SELECT count(mid) FROM rental where cid = ? and date_in = null";
    private PreparedStatement _count_rentals_statement;

    private String _customer_sql = "SELECT * FROM CUSTOMERS WHERE cid = ?" ;
    private PreparedStatement _customer_statement;

    private String _plan_sql = "SELECT * FROM RENTAL_PLAN WHERE pid = ?";
    private PreparedStatement _plan_statement;

    private String _planall_sql = "SELECT * FROM RENTAL_PLAN";
    private PreparedStatement _planall_statement;

    private String _setplan_sql = "UPDATE CUSTOMERS SET pid = ? WHERE cid = ?";
    private PreparedStatement _setplan_statement;

    private SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-DD");
    private Date date = new Date();
    private String _return_movie_sql = "BEGIN TRANSACTION UPDATE MovieRentals "
      + "SET date_in = " + dateFormat.format(date) + " WHERE cid = ? and mid = ? COMMIT OR ROLLBACK";
    private PreparedStatement _return_movie_statement;

     private String _casts_actor_mid_sql = "SELECT x.role, y.* " 
      + "FROM casts x, actor y " 
      + "WHERE x.mid = ? AND x.pid = y.id";
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

    private String _customer_login_sql = "SELECT * FROM customers WHERE username = ? and password = ?";
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
        _director_mid_statement = _imdb.prepareStatement(_director_mid_sql);
        _count_rentals_statement = _customer_db.prepareStatement(_count_rentals_sql);
        _customer_statement = _customer_db.prepareStatement(_customer_sql);
        _plan_statement = _customer_db.prepareStatement(_plan_sql);
        _planall_statement = _customer_db.prepareStatement(_planall_sql);
        _setplan_statement = _customer_db.prepareStatement(_setplan_sql);

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
        // String remaining_query = "SELECT count(mid) FROM movierentals where cid = " + cid + " and status = 'open'";
        // PreparedStatement remain = _customer_db.prepareStatement(remaining_query);
        _count_rentals_statement.clearParameters();
        _count_rentals_statement.setInt(1, cid);
        ResultSet remain_results = _count_rentals_statement.executeQuery();
        remain_results.next();

        _customer_statement.clearParameters();
        _customer_statement.setInt(1, cid);
        ResultSet customer_results = _customer_statement.executeQuery();
        customer_results.next();
        String plan_id = customer_results.getString(9);

        _plan_statement.clearParameters();
        _plan_statement.setInt(1, Integer.parseInt(plan_id));
        ResultSet plans = _plan_statement.executeQuery();
        plans.next();

        int remaining = Integer.parseInt(plans.getString(4)) - Integer.parseInt(remain_results.getString(1));

        return (remaining);
        // return (Integer.parseInt(remain_results.getString(1)));
    }

    public String helper_compute_customer_name(int cid) throws Exception {
        /* you find  the first + last name of the current customer */
        _customer_statement.clearParameters();
        _customer_statement.setInt(1, cid);
        ResultSet customer_results = _customer_statement.executeQuery();
        customer_results.next();
        return (customer_results.getString(4) + " " + customer_results.getString(5));
    }

    public boolean helper_check_plan(int plan_id) throws Exception {
        /* is plan_id a valid plan id ?  you have to figure out */
        _planall_statement.clearParameters();
        ResultSet plan_ids = _planall_statement.executeQuery();
        while (plan_ids.next()) {
            if (Integer.parseInt(plan_ids.getString(1)) == plan_id) {
                return true;
            }
        }
        return false;
    }

    public boolean helper_check_movie(int mid) throws Exception {
        /* is mid a valid movie id ? you have to figure out  */
        return true;
    }

    private int helper_who_has_this_movie(int mid) throws Exception {
        /* find the customer id (cid) of whoever currently rents the movie mid; return -1 if none */
        return (77);
    }

    /**********************************************************/
    /* login transaction: invoked only once, when the app is started  */
    public int transaction_login(String name, String password) throws Exception {
        /* authenticates the user, and returns the user id, or -1 if authentication fails */

        /* Uncomment after you create your own customers database */
       
        int cid;
        String username;
        String pass;

        _customer_login_statement.clearParameters();
        _customer_login_statement.setString(1,name);
        _customer_login_statement.setString(2,password);
        ResultSet cid_set = _customer_login_statement.executeQuery();
        /*
        if (cid_set.next()) cid = cid_set.getInt(1);
        else cid = -1;
        return(cid);
        
        return (55);
        */
        if (cid_set.next()) {
            username = cid_set.getString(2);
            pass = cid_set.getString(3);
            if (pass.equals(password)) {
                cid = cid_set.getInt(1);
                return cid;
            }
        }
        return(-1);
    }

    public void transaction_personal_data(int cid) throws Exception {
        /* println the customer's personal data: name, and plan number */
        System.out.println("Name: " + helper_compute_customer_name(cid) + ", Remaining Rentals: " + helper_compute_remaining_rentals(cid));
    }


    /**********************************************************/
    /* main functions in this project: */

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
        _setplan_statement.clearParameters();
        _setplan_statement.setInt(1, pid);
        _setplan_statement.setInt(2, cid);
        _setplan_statement.executeUpdate();
    }

    public void transaction_list_plans() throws Exception {
        /* println all available plans: SELECT * FROM plan */
        ResultSet plans = _planall_statement.executeQuery();
        while (plans.next()) {
            System.out.println("ID: " + plans.getString(1) + ", Name: " + plans.getString(2) + ", Cost: " + plans.getString(3) + ", Max Rentals: " + plans.getString(4));
        }
    }
    
    public void transaction_list_user_rentals(int cid) throws Exception {
        /* println all movies rented by the current user*/
    }

    public void transaction_rent(int cid, int mid) throws Exception {
        /* rend the movie mid to the customer cid */
        /* remember to enforce consistency ! */
        String midExists_sql = "SELECT * FROM movie WHERE id = ?";
        String _isRented_sql = "SELECT count(*) FROM rental WHERE mid = ? AND date_in ISNULL";
        String getPlan_sql = "SELECT max_rentals FROM customers c, rental_plan WHERE cid = ? AND c.pid = rental_plan.pid"; //change pid to plan name
        String getRented_sql = "SELECT count(*) FROM rental WHERE cid = ? AND date_in ISNULL";
        String updateRental_sql = "INSERT INTO rental (mid, cid, date_out, date_in, rid) Values(?, ?, ?, NULL, (SELECT count(*) FROM rental) + 1)";

        PreparedStatement midExists_statement = _customer_db.prepareStatement(midExists_sql);
        midExists_statement.clearParameters();
        midExists_statement.setInt(1, mid);

        PreparedStatement _isRented_statement = _customer_db.prepareStatement(_isRented_sql);
        _isRented_statement.clearParameters();
        _isRented_statement.setInt(1, mid);

        PreparedStatement getPlan_statement = _customer_db.prepareStatement(getPlan_sql);
        getPlan_statement.clearParameters();
        getPlan_statement.setInt(1, cid);

        PreparedStatement getRented_statement = _customer_db.prepareStatement(getRented_sql);
        getRented_statement.clearParameters();
        getRented_statement.setInt(1, cid);

        PreparedStatement updateRental_statement = _customer_db.prepareStatement(updateRental_sql);
        updateRental_statement.clearParameters();
        updateRental_statement.setInt(1, mid);
        updateRental_statement.setInt(2, cid);
        java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
        updateRental_statement.setDate(3, sqlDate);

        ResultSet realMovie = midExists_statement.executeQuery();

        if (realMovie.next()) {
            ResultSet movieStatus = _isRented_statement.executeQuery();
            if (movieStatus.next()) {
                if (movieStatus.getInt(1) == 1) {
                    System.out.println("Movie is UNAVAILABLE");
                }
                else {
                    ResultSet maxMovies = getPlan_statement.executeQuery();
                    ResultSet currRented = getRented_statement.executeQuery();
                    maxMovies.next();
                    currRented.next();
                    if (maxMovies.getInt(1) > currRented.getInt(1)) {
                        _begin_transaction_read_write_statement.executeUpdate();
                        updateRental_statement.executeUpdate();
                        _commit_transaction_statement.executeUpdate();
                        System.out.println("Success!");
                    }
                    else {
                        System.out.println("You cannot rent anymore movies! Return one before renting again!");
                    }

                }
            }

        }
        else {
            System.out.println("INVALID MovieID");
        }

    }


      public void transaction_return(int cid, int mid) throws Exception {
    /* return the movie mid by the customer cid */
    // if the user doesn't enter a number, it already displays their current rentals in VideoStore.java
    
    // in Rental table, update status to 'closed'
    _return_movie_statement.clearParameters();
    _return_movie_statement.setInt(1, cid);
    _return_movie_statement.setInt(2, mid);
    _return_movie_statement.execute();
      
  }

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
    if (!movie_set.next()) {
      return;
    }
    _fs_director_set.next();
    _fs_actor_set.next();
    int mid = -1;
    while (!movie_set.isAfterLast()) {
      if (movie_set.getInt(1) != mid) {
        mid = movie_set.getInt(1);
        System.out.println("ID: " + mid + " NAME: "
            + movie_set.getString(2) + " YEAR: "
            + movie_set.getString(3));
      }
      if (!_fs_director_set.isAfterLast() && _fs_director_set.getInt(1) == mid) {
        System.out.println("\t\tDIRECTOR: " + _fs_director_set.getString(4)
        + " " + _fs_director_set.getString(3));
        _fs_director_set.next();
        continue;
      } else if (!_fs_director_set.isAfterLast() && _fs_director_set.getInt(1) < mid) {
        _fs_director_set.next();
        continue;
      } else if (!_fs_actor_set.isAfterLast() && _fs_actor_set.getInt(1) == mid) {
        System.out.println("\t\tACTOR: " + _fs_actor_set.getString(4) 
        + " " + _fs_actor_set.getString(5) + " | ROLE: " + _fs_actor_set.getString(2));
        _fs_actor_set.next();
        continue;
      } else if (!_fs_actor_set.isAfterLast() && _fs_actor_set.getInt(1) < mid) {
        _fs_actor_set.next();
        continue;
      }
      movie_set.next();
    }
    System.out.println();
    _fs_director_set.close();
    _fs_actor_set.close();
    movie_set.close();
  }

}
