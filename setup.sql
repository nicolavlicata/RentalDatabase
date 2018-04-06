CREATE DATABASE CUSTOMER;

CREATE TABLE Movie (
	id INT PRIMARY KEY, 	
	name VARCHAR(200), 
	year INT
);

CREATE TABLE Rental_Plan (
	pid integer PRIMARY KEY,
	plan_name varchar UNIQUE NOT NULL,
	cost int NOT NULL,
	max_rentals int NOT NULL
);

CREATE TABLE Customers (
	cid integer PRIMARY KEY,
	username varchar,
	password varchar,
	fname varchar,
	lname varchar,
	address varchar,
	city varchar,
	country varchar,
	pid integer REFERENCES Rental_Plan (pid)
);

CREATE TABLE Rental (
	rid int PRIMARY KEY,
	mid int REFERENCES Movie(id),
	cid integer REFERENCES Customers(cid),
	date_out date,
	date_in date
);

INSERT INTO Rental_Plan VALUES (1, 'Basic', 10, 1);
INSERT INTO Rental_Plan VALUES (2, 'Premium', 15, 2);
INSERT INTO Rental_Plan VALUES (3, 'Employee', 0, 9999);

INSERT INTO Customers VALUES (1, 'jb1', 'password', 'Jack', 'Blanc', '885 Rubis Dr', 'Sunnyvale', 'USA', 1);
INSERT INTO Customers VALUES (2, 'dpc', 'blueberrys', 'Dylan', 'Collins', '111 Fake St', 'Boston', 'USA', 1);
INSERT INTO Customers VALUES (3, 'phillerj', 'best_password', 'Phil', 'Jackson', '1 Some Rd', 'Seattle', 'USA', 1);
INSERT INTO Customers VALUES (4, 'mWhite', 'test', 'Mary', 'White', '42 Real Dr', 'San Diego', 'USA', 2);
INSERT INTO Customers VALUES (5, 'oldschoolethel', 'secure', 'Ethel', 'Palmer', '7 Best Av', 'Ontario', 'Canada', 2);
INSERT INTO Customers VALUES (6, 'leung.an', 'benlerner', 'Andrew', 'Leung', '1 Street Lane Circle Dr', 'Eastford', 'USA', 1);
INSERT INTO Customers VALUES (55, 'a', 'a', 'b', 'c', '885 Rubis Dr', 'Sunnyvale', 'USA', 1);

INSERT INTO Rental VALUES (1, 200, 1, '02-10-2018', '02-20-2018');
INSERT INTO Rental VALUES (2, 100, 1, '02-10-2018', '02-20-2018');
INSERT INTO Rental VALUES (3, 101, 1, '02-10-2018', NULL);
INSERT INTO Rental VALUES (4, 123, 2, '02-20-2018', '02-20-2018');
INSERT INTO Rental VALUES (5, 123, 2, '02-20-2018', '02-22-2018');
INSERT INTO Rental VALUES (6, 124, 2, '10-10-2017', '11-1-2017');
INSERT INTO Rental VALUES (7, 101, 3, '02-09-2018', '02-09-2018');
INSERT INTO Rental VALUES (8, 124, 3, '02-01-2018', NULL);

\copy movie from 'C:/Users/Andrew Leung/Documents/2017-2018/Spring/Database Design/imdb2015/movie.txt' with delimiter '|' null as ''






