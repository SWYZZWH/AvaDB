### Notes

#### Python Version

3.11.0

#### Limited Memory

- Ava DB is designed for limited memory space. Only a constant number(ideally 1) of records will be loaded into memory at one time.
- Each record will be stored in a file, thus a great number of inodes will be used. For XFS and Btrfs, the amount of inodes is 2^64. For ext4, we might have to manually configure it to 2^64(however
  for our dataset the default value 2^32 also works).
- We should always return a file (by `send_file` in flask) as a response for any query. Directly return a json file is not allowed due to the limitation of memory.
- All records of the same table should be stored under the same file. To iterate through all files with limited memory, call `os.scandir` or `os.walkdir` (which calls `readdir(3)` behind the scene).
  Do not use `os.listdir`.

#### CLI

1. Start CLI
    ```bash
    chmod +x ${path_to_run_cli}
    ./${path_to_run_cli}
    ```
    Or
    ```bash
    cd ${project_root_dir}
    python3 -m cli.cli
    ```

2. Send Query

    ```bash
    >>> {"db_type": "nosql", ${query}}
    >>> {"db_type": "sql", ${query}}
    ```
   
