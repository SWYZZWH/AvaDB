# Project Proposal

### System Design

The whole system consists of three parts:

1.  SQL database
2.  NoSQL database
3.  Command line interface (CLI)



#### SQL Database

**Data Model**

Each row in a table represents an entry.
Tables are organized in a fixed schema that outlines the type and constraints of each
column.
Each entry will be assigned a PK (Primary Key) per table.
Foreign Keys will be used to represent relationships between tables.
Storage
Data will be stored in tables, Each table will be a separate file or set of files managed by the
SQL database engine. We don't manage these files directly.
Programming Language
SQL-based database will be implemented by Java, since the language has better
performance.


**Supported Operations**

SQL naturally supports JOIN operations, which allows for combining rows from two or more
tables based on a related column.
The other operations mentioned
projection
filtering
grouping
aggregation
ordering
insertion
updation
deletion are also fully supported in SQL databases.
"JOIN" will be supported in SQL database, since there are multiple tables can be operated.

**Big Data**

SQL database is able to store large volumes of data efficiently, but after the amount of
data reaches to million, optimizations such as indexing, caching, and partitioning can be
used().
SQL database allows transactions, locking, and other concurrency controls, to manage
multiple simultaneous operations.

**Other Details**

SQL database will have a local interface to issue commands and show query results.


#### NoSQL Database

**Data Model**

-   Each entry will be presented as an object. Each object will be stored as a JSON document.
-   Objects in the same class will be stored together as a collection.
-   Every entry will be assigned with a unique ID.
-   The entry will hold the IDs of other entries as JSON fields. 

**Storage**

We suppose every single entry can be loaded into memory. Each JSON document will be stored in a distinct file. Objects in the same collection will be stored under the same folder.

**Programming Language**

NoSQL database will be implemented with Python.

**Supported Operations**

"JOIN" is not supported by our NoSQL database (No tables to join for NoSQL!)

Other operations are supported, including:

-   projection
-   filtering
-   grouping
-   aggregation
-   ordering
-   Insertion, updation, and deletion are all supported.

**Big Data**

Only one JSON document will be loaded into memory for each thread. The JSON object will be cleared or replaced before loading the next JSON document. Only IDs of different objects will be kept in memory at the same time.

**Other Details**

NoSQL Database will be served as an HTTP server and handle HTTP requests only.



#### CLI

It's designed as a user interface to communicate with SQL or NoSQL servers. Queries will be sent by HTTP protocol, while the query results will be parsed and presented in standard output.

The CLI will be implemented with Python.



### Query Language

We'd like to propose a new query language that could be used for both SQL and NoSQL databases. 

The new query language is a declarative language like SQL. We will focus on querying results instead of querying procedures. SQL is powerful; however, NoSQL databases don't directly support SQL syntax, while a group of syntaxes is impossible to be supported by NoSQL (e.g., "JOIN"). Meanwhile, supporting the full set of SQL is an overkill for this project. 

We decided to design a **JSON-based** query language. Each query is a JSON document and can be processed with both SQL and NoSQL databases. The query language will be similar to Mongodb querying language while it's language-agnostic and simpler.

Natural language is not considered due to the ambiguity, context-dependency, and fluidity.

### Dataset

Our dataset is selected from [Kaggle](https://www.kaggle.com/datasets/joebeachcapital/ign-games). 

>   It contains 18625 data points with various features such as release dates with different platforms along with IGN scores. All the lines are fully filled. There are not any null values.



### Test Cases

1.   **Projection**

"Retrieve the title, platform, and release year of all games."

```sql
SELECT Title, Platform, ReleaseYear
FROM ign_games;
```

2.   **Filtering**

"Retrieve the titles and scores of games that have a score greater than 8.5 and were marked as Editor's Choice."

```sql
SELECT Title, Score
FROM ign_games
WHERE Score > 8.5 AND EditorsChoice = 'Yes';
```

3.   **Grouping**

"Retrieve the count of games for each platform."

```sql
SELECT Platform, COUNT(*)
FROM ign_games
GROUP BY Platform;
```

4.   **Aggregation**

"Retrieve the average score of games for each platform."

```sql
SELECT Platform, AVG(Score) AS AverageScore
FROM ign_games
GROUP BY Platform;
```

5.   **Ordering**

"Retrieve the titles and release years of all games, ordered by release year in descending order and then by title in ascending order."

```sql
SELECT Title, ReleaseYear
FROM ign_games
ORDER BY ReleaseYear DESC, Title ASC;
```

6.   **Join**

"Retrieve pairs of games that have the same score but different titles."

```sql
SELECT a.Title AS Game1Title, b.Title AS Game2Title, a.Score
FROM ign_games a
JOIN ign_games b ON a.Score = b.Score AND a.Title != b.Title;
```



### Team Work

| Tasks                           | Assignee     |
| ------------------------------- | ------------ |
| Design Query Language           | @SWYZZWH  |
| Design Data Model & Import Data | Zhenyu Xiong |
| Design Testcases                | @SWYZZWH  |
| Implement SQL Database          | Zhenyu Xiong |
| Implement NoSQL Database        | @SWYZZWH  |
| Implement CLI                   | @SWYZZWH  |
| Draft Project Proposal          | @SWYZZWH  |
| Draft Midterm Report            | Zhenyu Xiong |
| Draft Final Report              | @SWYZZWH  |
| Make Demo Video                 | Zhenyu Xiong |

