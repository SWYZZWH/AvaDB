
### Usage

#### Import Data

TODO

#### DB Server

1. Start

    ```bash
    chmod +x scripts/start_all_server.sh
    ./scripts/start_all_server.sh
    ```
   or start sql/nosql server only
    ```bash
    chmod +x scripts/start_sql_server.sh
    ./scripts/start_sql_server.sh
    ```
    ```bash
    chmod +x scripts/start_sql_server.sh
    ./scripts/start_nosql_server.sh
    ```

2. Check Log

   Logs are under project root dir. Namely `AvA DB nosql.log`, `AvA DB sql.log`, and `AvA DB cli.log`.

#### CLI

1. Start CLI
    ```bash
    chmod +x scripts/run_cli.sh
    ./scripts/run_cli.sh
    ```
   Or
    ```bash
    cd ${project_root_dir}
    python3 -m cli.cli
    ```

2. Send Query

   example queries are in cli/test_query_sql.txt and cli/test_query_nosql.txt, try copy-paste them into cli and have fun!

    ```bash
    >>> {"type": "sql", "create": {"table_name": "test_cli", "fields": [{"col1": "int"}, {"col2": "str"}]}}
    ```

### Important Notes

#### Python Version

3.11.0

#### Limited Memory

- Ava DB is designed for limited memory space. Only a constant number(even 1) of records will be loaded into memory at one time.
- In worst case, each record will be stored in a file, thus a great number of inodes will be used. For XFS and Btrfs, the amount of inodes is 2^64. For ext4, we might have to manually configure it to 2^64(however
  for our dataset the default value 2^32 also works).
- We should always return a file (by `send_file` in flask) as a response for any query. Directly return a json file is not allowed due to the limitation of memory.
- All records of the same table should be stored under the same file. To iterate through all files with limited memory, call `os.scandir` or `os.walkdir` (which calls `readdir(3)` behind the scene).
  Do not use `os.listdir`.

### Requirements

V1

1. Dataset Size:
   "Your system should not load the entire database into the memory and process queries and data modifications on the entire database. Instead, you should assume that the database may potentially
   handle a large amount of data that might not fit in the main memory "
    - Your dataset should demonstrate the above guidline. Students are free to choose the dataset dimensions from the real world datasets. You should assume ideal size of your memory for your project
      and the size of the dataset should be very big compared to memory. Assume a student chooses size of the memory used for execution of the queries is 4MB then datset should not be less than 20MB (
      Please note that this size is just mentioned as an example and you may have different size for your data. (Even when you are trying to implement query like "select * from All_Students", you
      should ensure that you don't load the entire data into memory).


2. Usage of Pandas: Pandas library can be used for this project but the scope is limited. All the queries executions should be performed in the limited memory without loading the entire dataset.
   Inbuilt operations of pandas cannot be used like "join" cannot be used to perform the operation. Joining algorithm, mapreduce techniques will be further taught in the course which will give idea on
   how to implement join methods.


3. Storage System: Students are free to design their storage system. A dataset can be stored as a single file or can be split into multiple files. Processing should be done in chunks.


4. Query Language: The language should be different from existing query language. Design a query language like human conversations with some keywords and map them to process the query using
   programming languages.

5. Web Application: Should provide User interface for your project. Should be able to fetch results using buttons. For example, displaying the entire table in the UI and providing UI buttons to add
   data, delete data or modify data. These buttons in the frontend should create an API call to your database backend. Given example is one of the examples of UI design, students are free to showcase
   their creativity in designing the UI.


6. Project Teams: Requirements mentioned in the guidlines are minimum requirements for grading. Students are given chance to choose their teamsize. So there will be no change in requirements for
   single person or multi person team.

V2

1. Usage of inbuilt libraries is very limited. Inbuilt libraries should not be used for performing operations like join, sort etc. for Example, pandas has .join() and .merge() which should not be used
   for performing those operations.

2. Logic for those operations needs to be implemented and should be able to demonstrate in code when asked.

3. Again pointing to the same point, key challenge of this project is to address the issue of memory management. You should assume you have a limited memory to perform operations. If your datasize is
   5MB and your memory assumption is 1MB, then your dataset should be divided into 5 different chunks and then perform operations.

4. Best approach for this project is to store data as chunks rather than reading as chunks using builtin methods like read_csv(n_chunks = ...). So that you have control over data processing as chunks.
   You can also store intermediate results on disk and construct final results from intermediate results again by reading them from the disk

### Testcases

TODO


### GPTs link

https://chat.openai.com/g/g-weihxGWPJ-avadb-project-manager
```

