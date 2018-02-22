CREATE DATABASE rental_service;

CREATE TABLE Movie (
	id int, 
	name varchar(200), 
	year int,
	PRIMARY KEY(id)
);
-- file path will vary depending on system
\copy movie from 'C:/Users/jblan/Desktop/imdb-cs3200/movie.txt' with delimiter '|' null as ''

-- does not allow you to import the data with "is_rented" as a column

ALTER TABLE movie
ADD COLUMN "is_rented" BOOLEAN DEFAULT FALSE;

CREATE TABLE Rental_Plan (
	plan_name varchar,
	cost int,
	max_rentals int,
	PRIMARY KEY (plan_name)
);

-- table name "user" invalid

CREATE TABLE Customer (
	username varchar,
	password varchar,
	fname varchar,
	lname varchar,
	address varchar,
	city varchar,
	country varchar,
	plan varchar,
	PRIMARY KEY (username),
	FOREIGN KEY (plan) REFERENCES Rental_Plan(plan_name)
);

CREATE TABLE Rental (
	movieid int,
	username varchar,
	date_out date,
	date_in date,
	FOREIGN KEY (movieid) REFERENCES Movie(id),
	FOREIGN KEY (username) REFERENCES Customer(username)
);

-- Plans will be one of 'Basic' or 'Premium'
INSERT INTO Rental_Plan VALUES ('Basic', 10, 1);
INSERT INTO Rental_Plan VALUES ('Premium', 15, 2);

INSERT INTO Customer VALUES ('jb1', 'password', 'Jack', 'Blanc', '885 Rubis Dr', 'Sunnyvale', 'USA', 'Basic');
INSERT INTO Customer VALUES ('dpc', 'blueberrys', 'Dylan', 'Collins', '111 Fake St', 'Boston', 'USA', 'Premium');
INSERT INTO Customer VALUES ('phillerj', 'best_password', 'Phil', 'Jackson', '1 Some Rd', 'Seattle', 'USA', 'Basic');
INSERT INTO Customer VALUES ('mWhite', 'test', 'Mary', 'White', '42 Real Dr', 'San Diego', 'USA', 'Premium');
INSERT INTO Customer VALUES ('oldschoolethel', 'secure', 'Ethel', 'Palmer', '7 Best Av', 'Ontario', 'Canada', 'Basic');

INSERT INTO Rental VALUES ('100', 'jb1', '02-10-2018', '02-20-2018');
-- movie is populated already







