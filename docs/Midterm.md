## Midterm Report



### DataSet 

Our dataset is decided and looks like follows:

|       | **score_phrase** | **title**                                            | **url**                                                      | **platform**     | **score** | **genre**  | **editors_choice** | **release_year** | **release_month** | **release_day** |
| ----- | ---------------- | ---------------------------------------------------- | ------------------------------------------------------------ | ---------------- | --------- | ---------- | ------------------ | ---------------- | ----------------- | --------------- |
| **0** | Amazing          | LittleBigPlanet PS Vita                              | /games/littlebigplanet-vita/vita-98907                       | PlayStation Vita | 9.0       | Platformer | Y                  | 2012             | 9                 | 12              |
| **1** | Amazing          | LittleBigPlanet PS Vita -- Marvel Super Hero Edition | /games/littlebigplanet-ps-vita-marvel-super-hero-edition/vita-20027059 | PlayStation Vita | 9.0       | Platformer | Y                  | 2012             | 9                 | 12              |
| **2** | Great            | Splice: Tree of Life                                 | /games/splice/ipad-141070                                    | iPad             | 8.5       | Puzzle     | N                  | 2012             | 9                 | 12              |



### DataModel

**Relation(SQL)**
Below is the file structure of the Relation Database, for each folder or directory, we consider it as a table, and every data row will be stored in a single csv file.

```
Root_Directory
 __ IGN_Game (This is a folder, equivalent to a table)
     __ 001.csv (This is a csv file, equivalent to a single row of data)
     __ 002.csv
     __ 003.csv
    ...
```

```csv sample
ScorePhrase,	     Title,						URL,								  Platform,			Score,		Genre,				EditorsChoice,		   ReleaseDate
Amazing,	LittleBigPlanet PS Vita,	/games/littlebigplanet-vita/vita-98907,	   PlayStation Vita,	 9.0,	   Platformer,				  Y,				2012-09-12
```

```csv sample
ScorePhrase,	                  Title,						                   URL,							Platform,			Score,		Genre,				EditorsChoice,		   ReleaseDate
Amazing,	LittleBigPlanet PS Vita -- Marvel Super Hero Edition,	/games/littlebigplanet-vita/vita-98907,	   PlayStation Vita,	 9.0,	   Platformer,				  Y,				2012-09-12
```

```csv sample
ScorePhrase,	     Title,						URL,				  Platform,			Score,		Genre,		EditorsChoice,		ReleaseDate
Great,	       Splice: Tree of Life,	/games/splice/ipad-141070,	    Ipad,	         8.5,	   Puzzle,			N,				2012-09-12
```



**NoSQL**

The sample of our dataset above will be represented by 3 json files in our NoSQL database. All these json files are stored under the same folder called `ign_game` which is also the table name.

```json
{
	score_phrase: "Amazing",
    title: "LittleBigPlanet PS Vita",
    url: "/games/littlebigplanet-vita/vita-98907",
    platform: "PlayStation Vita",
    score: 9.0,
    genre: "Platformer",
    editors_choice: "Y",
    release_year: 2012,
    release_month: 9,
    release_day: 12,
}
```



```json
{
	score_phrase: "Amazing",
    title: "LittleBigPlanet PS Vita -- Marvel Super Hero Edition",
    url: "/games/littlebigplanet-vita/vita-98907",
    platform: "PlayStation Vita",
    score: 9.0,
    genre: "Platformer",
    editors_choice: "Y",
    release_year: 2012,
    release_month: 9,
    release_day: 12,
}
```



```json
{
	score_phrase: "Great",
    title: "Splice: Tree of Life",
    url: "/games/splice/ipad-141070",
    platform: "iPad",
    score: 8.5,
    genre: "Puzzle",
    editors_choice: "N",
    release_year: 2012,
    release_month: 9,
    release_day: 12,
}
```



### Test Cases

We have designed the queries to test our implementations of database engines.

All the queries are currently represented in SQL. They will be translated into our query language later.



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



### Query Language Design

#### Description

We'd like to propose a new query language that could be used for both SQL and NoSQL databases. 

The new query language is a declarative language like SQL. We will focus on querying results instead of querying procedures. SQL is powerful; however, NoSQL databases don't directly support SQL syntax, while a group of syntaxes is impossible to be supported by NoSQL (e.g., "JOIN"). Meanwhile, supporting the full set of SQL is an overkill for this project. 

We decided to design a **JSON-based** query language. Each query is a JSON document and can be processed with both SQL and NoSQL databases. The query language will be similar to Mongodb querying language while it's language-agnostic and simpler.

Natural language is not considered due to the ambiguity, context-dependency, and fluidity.



#### Design

Each query can be represented by a single json object. All SQLs above will now be translated into our query language.

**Example 1: Projection**

```json
{
    tableType: "simple",
    srcTables: ["ign_games"],
    desiredColumns: ["0:Title", "0:Platform", "0:ReleaseYear"],
}
```

The json object represents a table. SQL or NoSQL databases should load data files according to table names in `srcTables`.

For joined table, there will be only more than one table name in `srcTables` while the tableType will be "joined".

To specify which columns we'd like to keep, a property `desiredColumns` is used.

Tables are referenced as a number according to their position in `srcTables`. 

Columns are always referenced in a format `{table_no}:{column_name}`. 

Note: column names are **case-insensitive**.



 **Example 2: Filtering** 

```json
{
    tableType: "simple",
    srcTables: ["ign_games"],
    desiredColumns: ["0:Title", "0:Score"],
    RowFilter: {
        op: "&&",
        v1: {
            op: ">",
            v1: "0:Score",
            v2: 8.5,
        },
        v2: {
            op: "==",
            v1: "0:EditorChoice",
            v2: "Yes",
        }
    }
}
```

`WHERE` conditions are translated into structural bool expressions.

For binary operator including ">", "<", "==", "!=", "+", "-", "*", "/", two properties (v1, v2) are provided. Each property can be a column reference, a literal value, or another statement.

For unary operator including "!", only one property (v1) is provided. 



**Example 3: Grouping**

```json
{
    tableType: "simple",
   srcTables: ["ign_games"],
    groupBy: "0:Platform"
}
```

Grouping is implemented by `groupBy` property which can be a reference to a column.



**Example 4: Aggregation**

```json
// Aggregation
{
 	tableType: "simple",
   srcTables: ["ign_games"],
    groupBy: "0:Platform",
    desiredColumns: ["0:Platform", {
        aggFunc: "avg",
        column: "0:Score",
        name: "AverageScore" // support renaming column
    }]
}
```

Aggregation should always be used together with grouping.

Columns in `desiredColumns` are columns to return. Each element could be a string (a column reference) or an object. The object is a wrapper for normal columns. More properties like a new name or a aggregation function can be added to the column.

Supported aggregation functions including:

-   AVG
-   COUNT
-   MIN
-   MAX
-   SUM



**Example 5: Ordering**

```json
{
    tableType: "simple",
   srcTables: ["ign_games"],
    desiredColumns: ["0:Title", "0:ReleaseYear"],
    orderedBy: [
        {
            column: "0:Title",
            order: "ASC"
        }, 
        {
            column: "0:ReleaseYear",
            order: "DESC"
        }
    ]
}
```

Results can be sorted by specifying`orderedBy`. Multiple columns can be added. The `order` property of each column can be `ASC` or `DESC`. The first column has the highest priority at ranking.



**Example 6: Joinning**

```json
{
    tableType: "joined",
    joinType: "inner",
    joinCondition: {
        op: "&&",
        v1: {
            op: "==",
            v1: "0:Score",
            v2: "1:Score",
        },
        v2: {
            op: "!=",
            v1: "",
            v2: "",
        }
    },
    srcTables: ["ign_games", "ign_games"],
    desiredColumns: ["0:Title", "1:Platform"],
}
```

Four types of join are supported:

-   inner
-   outer
-   left
-   right

`joinCondition` is also represented by structal expression. Logical operation including `&&` `||` are supported.



Our query language is flexible and powerful enough to support join of multiple tables.

```json
{
    tableType: "joined",
    joinType: "inner",
    joinCondition: {
        op: "&&",
        v1: {
            op: "==",
            v1: "0:Score",
            v2: "1:Score",
        },
        v2: {
            op: "!=",
            v1: "",
            v2: "",
        }
    },
    srcTables: ["ign_games", {
        tableType: "joined", 
        joinType: "inner",
        joinCondition: {
            op: "&&",
            v1: {
                op: "==",
                v1: "0:Score",
                v2: "1:Score",
            },
            v2: {
                op: "!=",
                v1: "",
                v2: "",
            }
    	},
        srcTables: ["ign_games", "ign_games"],
        desiredColumns: ["0:Title", "1:Platform"],
    }],
    desiredColumns: ["0:Title", "1:Platform"],
}
```



### Implementation

// TODO
