# Project Final Report

Check our database on [Github](https://github.com/SWYZZWH/AvaDB)!

[TOC]

### Introduction

// TODO



### Planned Implementation

// TODO



### Architecture Design

#### Overview

The project architecture is centered around the integration of SQL and NoSQL databases, providing a unified interface for data manipulation and query processing. It leverages Python for backend development, Flask for API handling, and a custom query language designed for optimized data handling in limited memory environments.



#### Key Components

1.  **Database Configuration (DBConfig):** Manages database settings, including type (SQL or NoSQL), port, directories for tables and metadata, supported field types, and file extensions.

2.  **Data Import (import_data.py):** Handles data import from CSV and JSON files, auto-detecting column types for CSV files and loading JSON files for NoSQL databases.

3.  **Logging (logger.py):** Provides a logging mechanism for tracking and debugging. Custom loggers for different components ensure detailed logging.

4.  **Server Management (app/server.py):** Uses Flask to handle API requests, enabling operations like file retrieval, data insertion, updating, deletion, table creation, and dropping.

5.  **Context Management (app/common/context/context.py):** Manages the application's runtime context, including configurations, database instances, table managers, and query engines.

6.  **Table Management (app/common/table/table_manager.py):** Manages tables, including creation, deletion, and metadata handling. Ensures data consistency and manages temporary tables for complex operations.

7.  **Command Line Interface (cli/cli.py):**

    Offers an interactive interface for users to directly communicate with the server. It translates command-line inputs into HTTP requests that are processed by `server.py`.

8.  **Query Engine(app/common/query/query_engine.py):**

    Processes and executes queries by interfacing with the SQL or NoSQL database. It plays a crucial role in the efficient handling of database queries.

9.  **Chunk Management(app/common/table/chunk_manager.py) :**

    Critical for handling large datasets in a limited memory space. This component ensures data is processed and stored in manageable chunks, allowing efficient data manipulation without overloading the system's memory. This approach is particularly important for operations like data import, query processing, and table management.



#### Flow Diagram

The flow diagram (presented below) illustrates the interaction between these components.

![Class Diagram](/Users/zhaoweihao/Downloads/Class Diagram (2).svg)

**Diagram Description**

1.  **CLI**
    -   interacts with users and send table operations or queries to flask server
    -   formats query result for human to read 
2.  **DBInterface**
    -   it's a flask server
    -   parses requests (CRUD operations, table creation/dropping)
    -   decides which database the CLI is talking to and send the request to corresponding database to handle
    -   formats the query result before sending back to CLI
3.  **Context**
    -   holds instance of query engine, table manager, logger, and config
    -   gets initialized when starting the flask server
    -   be passed to all other modules to use
4.  **TableManager**
    -   loads all info of tables from the disk and registers them in memory
    -   handles creation, deletion of tables
    -   offers apis to create a temporary table, all temporary tables will be deleted when starting the flask server
5.  **QueryEngine**
    -   parses queries and decides the execution plan
    -   uses `TableManipulator` to execute each step in the execution plan
6.  **TableManipulator**
    -   implements all operations including: projection/renaming/filtering/ for both SQL and NoSQL
    -   uses `ChunkManager` to create/load/update/delete chunks of tables on the disk
7.  **ChunkManager**
    -   manages all chunks of a table on the disk
    -   offers CRUD apis for chunks
8.  **Logging:**
    -   logs all operations across modules
    -   useful for debugging



### Implementation

#### Functionalities

// TODO



#### Tech Stack

1.   The project is mainly developed with `Python`. Some `Bash` scripts are also used for starting servers and cli.

2.   `Flask` framework is used to serve both SQL and NoSQL database engines.
3.   We use `Postman` to test apis offered by servers.
4.   `Pytest` is used for unittests.
5.   Version control system is `Git`. We use `Github` to collaborate.
6.    We use standard libs of `Python` to implement the entire project. No other third-party libs are used. (e.g. pandas)



#### Implementation Screenshots

**CLI**

![image-20231202185414310](/Users/zhaoweihao/Library/Application Support/typora-user-images/image-20231202185414310.png)

**Flask Server**

![image-20231202185523654](/Users/zhaoweihao/Library/Application Support/typora-user-images/image-20231202185523654.png)

**QueryEngine: handle query**

![image-20231202185716652](/Users/zhaoweihao/Library/Application Support/typora-user-images/image-20231202185716652.png)

**TableManipulator: merge_sort** 

![image-20231202185221532](/Users/zhaoweihao/Library/Application Support/typora-user-images/image-20231202185221532.png)

**TableManager**

![image-20231202185904201](/Users/zhaoweihao/Library/Application Support/typora-user-images/image-20231202185904201.png)

**Table**

![image-20231202190203691](/Users/zhaoweihao/Library/Application Support/typora-user-images/image-20231202190203691.png)

**ChunkManager**

![image-20231202190010949](/Users/zhaoweihao/Library/Application Support/typora-user-images/image-20231202190010949.png)



### Learning Outcomes

1.  **Designing a Unique Query Language**
    -   **Challenge:** Creating a query language distinct from existing ones, which needed to be expressive, clear, and easy to parse, presented a unique set of difficulties.
    -   **Solution:** We chose JSON as the format for our query language, striking a balance between expressiveness and simplicity, ensuring ease of parsing and clarity.
2.  **Handling Multiple Source Tables and Subqueries**
    -   **Challenge:** Managing references to specific fields across multiple tables and nested subqueries posed a significant challenge, particularly in maintaining clarity and accuracy for users and in the backend processing.
    -   **Solution:** We devised a set of naming conventions that could be seamlessly integrated into our query language and the query engine. This innovation greatly simplified referencing fields in complex query scenarios.
3.  **Integration of SQL and NoSQL**
    -   **Challenge:** A major challenge was integrating SQL and NoSQL databases to handle both CSV (structured) and JSON (possibly nested) data in a unified manner.
    -   **Solution:** We developed a system where components like the TableManipulator and ChunkManager are versatile enough to support both data formats. This approach minimized code duplication and streamlined our development process.
4.  **Memory Management and Chunk-Based Data Storage**
    -   **Challenge:** Given the constraint of limited memory, efficiently managing large datasets by storing and processing them in chunks was a significant technical challenge.
    -   **Solution:** We carefully designed the ChunkManager, Table, and TableManipulator classes, simplifying the implementation of complex operations like merge sorting on chunks. To ensure the reliability of each API, extensive unit testing was conducted. This comprehensive testing approach reduced the likelihood of errors when integrating different system components.



### Individual Contribution

| Tasks                           | Assignee     |
| ------------------------------- | ------------ |
| Design Query Language           | Weihao Zhao  |
| Design Data Model & Import Data | Zhenyu Xiong |
| Design Testcases                | Weihao Zhao  |
| Implement SQL Database          | Zhenyu Xiong |
| Implement NoSQL Database        | Weihao Zhao  |
| Implement CLI                   | Weihao Zhao  |
| Draft Project Proposal          | Weihao Zhao  |
| Draft Midterm Report            | Zhenyu Xiong |
| Draft Final Report              | Weihao Zhao  |
| Make Demo Video                 | Zhenyu Xiong |



### Conclusion

This project has been an exemplary journey in database management system design, especially in the context of an educational setting. By successfully integrating SQL and NoSQL databases and developing a custom JSON-based query language, our team not only met but exceeded the project's requirements. This accomplishment underscores our ability to address complex challenges, such as handling various data formats and implementing efficient memory management in a constrained environment.

The collaboration throughout this project was solid and effective, with each team member contributing significantly to the project's success. We navigated the complexities of database systems, developing solutions for managing multiple source tables, subqueries, and implementing chunk-based data storage. These experiences have greatly enhanced our understanding of databases, showcasing our adaptability and problem-solving skills in the face of technical challenges.

In essence, this project has not only fulfilled its academic objectives but has also provided us with invaluable insights into the practical aspects of database system design and development, laying a strong foundation for our future endeavors in this field.



### Future Scope

1.   **Concurrency Control**

     Introducing locks for operations on tables is an essential aspect of concurrency control in database systems, ensuring data integrity and consistency during concurrent access.

2.   **Implementation of Mock Server in CLI:**

     A mock server setup in `cli/mock_server.py` is planned but not yet implemented. Completing this would enhance testing and simulation capabilities for the CLI.

3.   **Type Conversion Handling:**

     Handling potential type conversions (e.g., int to float) during data manipulations is an area for development. This addition would increase the robustness and accuracy of data processing.

4.   **Completion of Test Cases:**

     Developing comprehensive test cases is crucial for ensuring the system's reliability and performance under various scenarios.
