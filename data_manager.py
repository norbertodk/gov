import sqlite3,sys
from sqlite3 import Error

 
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
 
    return None
 
 
def select_all_tasks(conn):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM tasks")
 
    rows = cur.fetchall()
 
    for row in rows:
        print(row)
 
 
def select_invoices_by_quantity(conn, minimum_total):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param priority:
    :return:
    """
    
    insert_sql = ''' INSERT INTO country_purchases(billingcountry,total_purchases)
              VALUES(?,?) '''
    delete_sql = ''' DELETE FROM country_purchases '''

    
    
    cur = conn.cursor()
    cur.execute("SELECT billingcountry, count(*) tot FROM invoices GROUP BY billingcountry HAVING count(*)>?", (minimum_total,))

    rows = cur.fetchall()

    delete_from_table(conn,delete_sql)
    
    f = open("C:\Python27\Lib\myresults\countryNboughts.csv","w+");
    f.write("country,purchases\n");
    for row in rows:
        f.write( str(row[0]) + "," + str(row[1]) + "\n" );
        new_row = (row[0],row[1])
        insert_into_table(conn,new_row,insert_sql)
    f.close();

 
def select_items_purchased(conn):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param priority:
    :return:
    """
    
    insert_sql = ''' INSERT INTO country_items_purchased(billingcountry,total_items_purchased)
              VALUES(?,?) '''
    delete_sql = ''' DELETE FROM country_items_purchased '''

    
    
    cur = conn.cursor()
    cur.execute("SELECT BillingCountry, sum(Quantity) qtty FROM invoices i left join invoice_items it on i.InvoiceId = it.InvoiceId GROUP BY billingcountry ")

    rows = cur.fetchall()

    delete_from_table(conn,delete_sql)
    
    f = open("C:\Python27\Lib\myresults\CcountryItemsPurchased.csv","w+");
    f.write("country,itemsPurchased\n");
    for row in rows:
        f.write( str(row[0]) + "," + str(row[1]) + "\n" );
        new_row = (row[0],row[1])
        insert_into_table(conn,new_row,insert_sql)
    f.close();

def select_best_seller(conn, country,year):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param priority:
    :return:
    """
    
    insert_sql = ''' INSERT INTO best_sellers(billingcountry,the_year, album, sales_qtty,rank) VALUES (?,?,?,?,?) '''
    delete_sql = ''' DELETE FROM best_sellers '''
    
    cur = conn.cursor()

    cur.execute("select t1.BillingCountry, t1.the_year, t2.album, sum(t1.Quantity) sales_qtty ,'ranker' from (select i.BillingCountry, strftime('%Y', i.InvoiceDate) the_year,it.trackid,it.Quantity,1 from invoices i left join invoice_items it on i.InvoiceId = it.InvoiceId where upper(BillingCountry) = ? and strftime('%Y',invoicedate) > ?) t1 join (select t.trackid, t.albumid , a.title album from tracks t join genres g on t.genreid = g.genreid and upper(g.name) = upper('rock') left join albums a on t.albumid = a.albumid) t2 on t1.trackid = t2.trackid group by t1.BillingCountry, t1.the_year, t2.album ", (country.upper(),'''+year+'''))
    #cur.execute("select BillingCountry, the_year, album, sales_qtty, dense_rank() over ( order by sales_qtty desc) rr from (select t1.BillingCountry, t1.the_year, t2.album, sum(t1.Quantity) sales_qtty from (select i.BillingCountry, strftime('%Y', i.InvoiceDate) the_year,it.trackid,it.Quantity,1 from invoices i left join invoice_items it on i.InvoiceId = it.InvoiceId where upper(BillingCountry) = 'ARGENTINA' and strftime('%Y',invoicedate) > '2012') t1 join (select t.trackid, t.albumid , a.title album from tracks t join genres g on t.genreid = g.genreid and upper(g.name) = upper('rock') left join albums a on t.albumid = a.albumid) t2 on t1.trackid = t2.trackid group by t1.BillingCountry, t1.the_year, t2.album ) t3")

    rows = cur.fetchall()

    delete_from_table(conn,delete_sql)
    
    f = open("C:\Python27\Lib\myresults\CcountryAlbumsPurchased.csv","w+");    
    f.write("country,the_year, album, sales_qtty,rank\n");
    for row in rows:
        f.write( str(row[0]) + "," + str(row[1]) + "," + str(row[2]) + "," + str(row[3]) + "," + str(row[4]) + "\n" );
        new_row = (row[0],row[1],row[2],row[3],row[4])
        #insert_into_table(conn,new_row,insert_sql)
    f.close();
    
def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def insert_into_table(conn, the_row, sql):
    """
    insert a new row
    :param conn:
    :param task:
    :param sql
    :return:
    """
 
    cur = conn.cursor()
    cur.execute(sql, the_row)
    return cur.lastrowid

def delete_from_table(conn, sql):

    cur = conn.cursor()
    cur.execute(sql)
                          
def main(country,year,db):
    database = "C:\\sqlite\db\chinook.db"
    database = db
 
    # create a database connection
    conn = create_connection(database)
    with conn:

        #print("1. Create table purchases by country " + str(argv))
        print("1. Create table purchases by country in db")
        sql_create_country_purchases_table = """ CREATE TABLE IF NOT EXISTS country_purchases (
                                                 BillingCountry NVARCHAR(40),
                                                 total_purchases NUMERIC(10,2)  NOT NULL
                                             ); """

        create_table(conn, sql_create_country_purchases_table)
        
        print("2. Query invoices by minimum quantity bought:")
        select_invoices_by_quantity(conn,0)

        print("3. Create table items purchased by country in db")
        sql_create_items_purchased_table = """ CREATE TABLE IF NOT EXISTS country_items_purchased (
                                                 BillingCountry NVARCHAR(40),
                                                 total_items_purchased NUMERIC(10,2)  NOT NULL
                                             ); """

        create_table(conn, sql_create_items_purchased_table)
        
        print("4. Query quantity of items purchased:")
        select_items_purchased(conn)

        print("5. Create tabel best sellers albums in dn")
        sql_create_best_sellers_table = """ CREATE TABLE IF NOT EXISTS best_sellers (
                                                 BillingCountry NVARCHAR(40),
                                                 the_year text,
                                                 album NVARCHAR(160),
                                                 sales_qtty NUMERIC(10,2),
                                                 rank NUMBERIC(10)
                                             ); """

        create_table(conn, sql_create_best_sellers_table)

        print("6. Query best sellers albums sales:")
        select_best_seller(conn,country,year)
        
    conn.close()

if __name__ == '__main__':
    main(sys.argv)
