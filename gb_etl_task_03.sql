-- create table
CREATE TABLE movies  (
movies_type text not null, 
director text not null, 
year_of_issue int not null, 
length_in_minutes int not null, 
rate int not null);

-- create table by years
CREATE TABLE movies_year_before_1990 (CHECK (year_of_issue <= 1990)) INHERITS (movies);
CREATE RULE year_before_1990 AS ON INSERT TO movies WHERE (year_of_issue <= 1990) DO INSTEAD INSERT INTO movies_year_before_1990 VALUES (NEW.*);

CREATE TABLE movies_year_1990_2000 (CHECK (year_of_issue BETWEEN 1990 AND 2000)) INHERITS (movies);
CREATE RULE year_1990_2000 AS ON INSERT TO movies WHERE (year_of_issue BETWEEN 1990 AND 2000) DO INSTEAD INSERT INTO movies_year_1990_2000 VALUES (NEW.*);

CREATE TABLE movies_year_2000_2010 (CHECK (year_of_issue BETWEEN 2000 AND 2010)) INHERITS (movies);
CREATE RULE year_2000_2010 AS ON INSERT TO movies WHERE (year_of_issue BETWEEN 2000 AND 2010) DO INSTEAD INSERT INTO movies_year_2000_2010 VALUES (NEW.*);

CREATE TABLE movies_year_2010_2020 (CHECK (year_of_issue BETWEEN 2010 AND 2020)) INHERITS (movies);
CREATE RULE year_2010_2020 AS ON INSERT TO movies WHERE (year_of_issue BETWEEN 2010 AND 2020) DO INSTEAD INSERT INTO movies_year_2010_2020 VALUES (NEW.*);

CREATE TABLE movies_year_after_2020 (CHECK (year_of_issue > 2020)) INHERITS (movies);
CREATE RULE year_after_2020 AS ON INSERT TO movies WHERE (year_of_issue > 2020) DO INSTEAD INSERT INTO movies_year_after_2020 VALUES (NEW.*);

-- create table by duration
CREATE TABLE min_upto_40 (CHECK (length_in_minutes <= 40)) INHERITS (movies);
CREATE RULE upto_40 AS ON INSERT TO movies WHERE (length_in_minutes <= 40) DO INSTEAD INSERT INTO min_upto_40 VALUES (NEW.*);

CREATE TABLE min_40_90 (CHECK (length_in_minutes BETWEEN 40 AND 90)) INHERITS (movies);
CREATE RULE between_40_90 AS ON INSERT TO movies WHERE (length_in_minutes BETWEEN 40 AND 90) DO INSTEAD INSERT INTO min_40_90 VALUES (NEW.*);

CREATE TABLE min_90_130 (CHECK (length_in_minutes BETWEEN 90 AND 130)) INHERITS (movies);
CREATE RULE between_90_130 AS ON INSERT TO movies WHERE (length_in_minutes BETWEEN 90 AND 130) DO INSTEAD INSERT INTO min_90_130 VALUES (NEW.*);

CREATE TABLE min_over_130 (CHECK (length_in_minutes > 130)) INHERITS (movies);
CREATE RULE over_130 AS ON INSERT TO movies WHERE (length_in_minutes > 130) DO INSTEAD INSERT INTO min_over_130 VALUES (NEW.*);

-- create table by rate
CREATE TABLE rate_upto_5 (CHECK (rate <= 5)) INHERITS (movies);
CREATE RULE rate_upto_5 AS ON INSERT TO movies WHERE (rate <= 5) DO INSTEAD INSERT INTO rate_upto_5 VALUES (NEW.*);

CREATE TABLE rate_5_8 (CHECK (rate BETWEEN 5 AND 8)) INHERITS (movies);
CREATE RULE rate_between_40_90 AS ON INSERT TO movies WHERE (rate BETWEEN 5 AND 8) DO INSTEAD INSERT INTO rate_5_8 VALUES (NEW.*);

CREATE TABLE rate_8_10 (CHECK (rate BETWEEN 8 AND 10)) INHERITS (movies);
CREATE RULE rate_between_8_10 AS ON INSERT TO movies WHERE (rate BETWEEN 8 AND 10) DO INSTEAD INSERT INTO rate_8_10 VALUES (NEW.*);

INSERT INTO movies (movies_type, director, year_of_issue, length_in_minutes, rate) VALUES 
('horror', 'Ivanov', 1989, 35, 7),
('comedy', 'Petrov', 1964, 43, 3),
('horror', 'Sidorov', 1989, 57, 4),
('romantic', 'Pugacheva', 1995, 34, 5),
('horror', 'Malinin', 1997, 93, 7),
('action', 'Kirkorov', 1999, 102, 6),
('horror', 'Petrov', 2001, 129, 7),
('action', 'Vasechkin', 2002, 131, 8),
('horror', 'Repin', 2003, 134, 7),
('comedy', 'Ivanov', 2011, 29, 7),
('horror', 'Panin', 2012, 56, 9),
('comedy', 'Shishkin', 2013, 78, 10),
('horror', 'Picasso', 2021, 156, 10),
('bio', 'Vodkin', 2022, 46, 9),
('bio', 'Savrasov', 2024, 97, 9),
('doc', 'Olga', 2024, 44, 12),
('doc', 'Anna', 2024, 95, 11);

SELECT * FROM ONLY movies;

SELECT * FROM movies;
SELECT * FROM rate_upto_5;
SELECT * FROM rate_5_8;
SELECT * FROM rate_8_10;
SELECT * FROM min_upto_40;
SELECT * FROM min_40_90;
SELECT * FROM min_90_130;
SELECT * FROM min_over_130;
SELECT * FROM movies_year_before_1990;
SELECT * FROM movies_year_1990_2000;
SELECT * FROM movies_year_2000_2010;
SELECT * FROM movies_year_2010_2020;
SELECT * FROM movies_year_after_2020;