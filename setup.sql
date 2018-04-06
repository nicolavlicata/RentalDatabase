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
	mid int,
	cid integer REFERENCES Customers(cid),
	date_out date,
	date_in date
);

\copy movie from 'C:/Users/Andrew Leung/Documents/2017-2018/Spring/Database Design/imdb2015/movie.txt' with delimiter '|' null as ''






