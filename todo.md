## Tags

@Todo, TODO
@Perf
@Memory
@Unsafe
@Hack
@Note

Everywhere when we load siblings and try to distribute stuff we can avoid collecting all cells at first and then trying to see if we can fit everything after distribution or create new page etc.. we can smartly check if after distribution also we can't fit everything so -->> don't try distribution unless it **helps**

have proper convention for enum and tagged enum field names. all capital or all small??

DONE All tests should save temp test files inside some directory which is ignored by git.

At some point we should have our own logger system.

Different Allocators for different usage patterns.

# Roadmap

1. DONE Relocate btree tests closer to btree and make current tests happy.
2. DONE Buffer pool manager which uses HEAP instead of current static allocated pages.
3. DONE Make btree tests more exhaustive.
    DONE 1. Handle errors
    DONE 2. Select should return the stuff.
    DONE 3. Support update operation.
    DONE 4. Test update operation.
4. DONE Update other commands to also accept arena allocator and use that to store stuff.
5. DONE Create a proper folder structure. Storage, main (connections, db instances, execution engine etc)
6. DONE Catalog manager for existing metadata page. (Catalog is present inside db instance.)
7. Support Execution plan creation and it's use.
   1. DONE Parser.
   2. CANCELLED Binder validates parsed statements and then binds stuff together.
   3. DONE Support create table query using parser and analyzer.
      1. DONE Support auto increment flag in parser
      2. W update create_table references everywhere.
   4. Query plan creation.
   5. Query plan optimization.
   6. Query plan execution. Volcano style query processing.
8. Update existing tests to select and check after insert, updates and deletes.
9. Support null values! Use bitmaps.
10. pagers get_page returns pointers instead copy of the page. have get_page and get_page_mut Reference counts are tracked to know if page can be freed, it can be written to etc..
11. DB instance locks database file until active.
12. Database Instance struct, connection struct and execution context structs.
13. Pull based execution model. Execution plan consisting steps, operators etc.
14. Ability to create database connections
15. Track transaction states, transaction manager which is needed for WAL
16. WAL implementation
17. Support parallel queries and connections. Multi threading!
    1.  Requires io to perform single write at a time.
    2.  Requires WAL to do single write at a time.

## WAL (Write ahead log)

Write ahead log records the changes done on data which exist on the cached pages but not yet written to the database file. When database restarts then at-first WAL entries get replayed (i.e persisted to disk). Incase when we are done with update query and we have sent a client response then we can't let go of cached page because when we load it again from disk it will be outdated. That's why we need to know whether cached page can be freed or not AND whether it can be read(concurrent read reference should not be returned if update query is still processing).

Each WAL entry has incremental transaction_id, target_table etc..

Each transaction should be replayed sequentially. consider, we insert on the table and later delete inserted rows. In order to delete we first need to have those rows in the first place!

```
struct WALEntry {
    transaction_id_t transaction_id;
    WALType operation_type;  // INSERT, DELETE, UPDATE, CREATE_TABLE, etc.
    string schema_name;
    string table_name;
    data_ptr_t operation_data;  // Serialized operation-specific data
    idx_t data_size;
    checksum_t checksum;  // For integrity verification
};

Size tracking: GetWALSize() and GetTotalWritten() for monitoring
Truncation support: Truncate(idx_t size) for rollback scenarios
```

While replaying the WAL we want to persist each transaction separately and in order and then mark that transaction replayed.

static unique_ptr<WriteAheadLog> Replay(FileSystem &fs, AttachedDatabase &database, const string &wal_path);
Enhanced Recovery Algorithm:
1. Validation phase: Verify WAL integrity and identify complete transactions
2. Dependency analysis: Build operation dependency graph
3. Parallel replay: Replay independent operations in parallel where safe
4. Checkpoint integration: Coordinate with existing checkpoints
5. Progress tracking: Mark replay progress to handle recovery interruptions
