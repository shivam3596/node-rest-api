# node-rest-api

Postgres Table Schema

```
CREATE TABLE person (
	person_id serial PRIMARY KEY,
	name VARCHAR ( 50 ) NOT NULL,
	city VARCHAR ( 50 ) NOT NULL
);

```

//SQL to create row in person table

INSERT INTO person (person_id, name, city) VALUES(1,'shivam chauhan','muzaffarnagar');

```

https://node-rest1.herokuapp.com/person/1
