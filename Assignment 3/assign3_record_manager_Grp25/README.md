# Record Manager

## How to run the code

To compile, build and run the test cases, use the following command

Type "make clean" to delete old compiled .o files.
Type "make" to compile
Type "make run" to run "test_assign3_1.c" file.
Type "make test_expr" to compile test expression related files including "test_expr.c".
Type "make run_expr" to run "test_expr.c" file.

## Solution Approach

## record_mgr.c

## Ranjitha:

**initRecordManager():**

- This function initializes the Record Manager, including the Storage Manager, to set up the necessary components for managing records in a database.
- It returns an RC_OK status upon successful initialization.

**shutdownRecordManager():**

- The shutdownRecordManager function is responsible for gracefully shutting down the Record Manager.
- It frees up allocated memory and ensures a clean exit by setting the recordManager variable to NULL.
- The function returns an RC_OK status to indicate successful shutdown.

**createTable():**

- The createTable function initializes a new table with the specified name and schema.
- It allocates memory for the record manager, initializes the buffer pool, and writes essential metadata to the first block of the associated page file.

**openTable():**

- The openTable function opens a table with the given name, initializes a scan on the table, and retrieves metadata from the first block of the page file to populate the table's schema and other essential information.

**closeTable():**

- The closeTable function gracefully closes a table by setting its relation metadata to NULL, ensuring that it can no longer be accessed, and returns RC_OK.

**deleteTable():**

- The deleteTable function deletes a table by removing its associated page file using the storage manager. It returns RC_OK upon successful deletion.

**getNumTuples():**

- The getNumTuples function returns the number of tuples (records) in a table referenced by the given RM_TableData.

## Pradaap

**insertRecord():**

- The insertRecord function inserts a new record into the specified table, updating the record's ID with the newly inserted record's location.

**deleteRecord():**

- The deleteRecord function marks a record identified by its Record ID for deletion in the specified table.

**updateRecord():**

- The updateRecord function modifies the data of a record referenced by the provided Record pointer within the specified table.

**getRecord():**

- The getRecord function retrieves a record with the given Record ID from the specified table, populating the provided Record structure with the retrieved data.

**startScan():** - The startScan function initializes a table scan with the provided condition, creating a new RecordManager structure for managing the scan's state.

**next():** - The next function advances a table scan to the next record satisfying the scan's condition, updating the provided Record structure with the retrieved data.

**closeScan():** - The closeScan function finalizes and closes a table scan, releasing associated resources and memory.

## Geeta

**getRecordSize():** - The getRecordSize function calculates and returns the size of a record in bytes based on the attributes and their data types defined in the provided schema.

**createSchema():** - The createSchema function dynamically allocates memory for a new schema and initializes its attributes, data types, type lengths, key size, and key attributes.

**freeSchema():** - The freeSchema function releases the memory occupied by a schema structure.

**createRecord():** - The createRecord function dynamically allocates memory for a new record, including space for the record's data.

**freeRecord():** - The freeRecord function releases the memory occupied by a record, including its data.

**getAttr():** - The getAttr function retrieves the value of a specified attribute from a given record within the context of the provided schema.

**setAttr():** - The setAttr function sets the value of a specified attribute in a given record within the context of the provided schema.

## record_mgr_helper.c

**findFreeSlot():**

- function iterates through the slots in a given page to find the first available slot for a record.
- It checks the presence of a tombstone marker ('+') at the beginning of each slot. If a slot is not marked ('+'), it is considered free, and its index is returned.
- If all slots are occupied, the function returns -1, indicating that no free slot is available.
