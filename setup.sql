CREATE DATABASE rental_service;

CREATE TABLE Movie (
	id int, 
	name varchar(200), 
	year int,
	is_rented boolean,
	PRIMARY KEY(id)
);
-- file path will vary depending on system
\copy movie from 'C:/Users/jblan/Desktop/imdb-cs3200/movie.txt' with delimiter '|' null as ''

CREATE TABLE Rental_Plan (
	plan_name varchar,
	cost int,
	max_rentals int,
	PRIMARY KEY (plan_name)
);

CREATE TABLE User (
	username varchar,
	password varchar,
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
	FOREIGN KEY (username) REFERENCES User(username)
);

INSERT INTO User VALUES ('jack blanc', 'password', '885 Rubis Dr', 'Sunnyvale', 'USA', 'Basic');
-- Plans will be one of 'Basic' or 'Premium'
INSERT INTO Rental_Plan VALUES ('Basic', 10, 1);
INSERT INTO Rental VALUES ('100', 'jack blanc', '02-10-2018', '02-20-2018');
-- movie is populated already