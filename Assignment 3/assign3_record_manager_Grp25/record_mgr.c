#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr_helper.c"

const int MAX_NUMBER_OF_PAGES = 100;
const int ATTRIBUTE_SIZE = 15; // Size of the name of the attribute

RecordManager *recordManager;

// ******** TABLE AND RECORD MANAGER FUNCTIONS ******** //

// This function initializes the Record Manager
extern RC initRecordManager(void *mgmtData)
{
    // Initializing Storage Manager
    initStorageManager();
    return RC_OK;
}

// This functions shuts down the Record Manager
extern RC shutdownRecordManager()
{
    // Freeing Record Manager memory
    free(recordManager);
    recordManager = NULL; // Set to NULL after freeing
    return RC_OK;
}

// This function creates a TABLE with table name "name" having schema specified by "schema"
extern RC createTable(char *name, Schema *schema)
{
    // Allocate memory space to the record manager custom data structure
    recordManager = (RecordManager *)malloc(sizeof(RecordManager));
    if (recordManager == NULL)
    {
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    // Initialize the Buffer Pool using LFU page replacement policy
    initBufferPool(&recordManager->bufferPool, name, MAX_NUMBER_OF_PAGES, RS_LRU, NULL);

    char data[PAGE_SIZE];
    char *pageHandle = data;

    // Initialize metadata in the buffer
    *(int *)pageHandle = 0; // Number of tuples
    pageHandle += sizeof(int);

    *(int *)pageHandle = 1; // First page is for schema and other metadata
    pageHandle += sizeof(int);

    *(int *)pageHandle = schema->numAttr; // Number of attributes
    pageHandle += sizeof(int);

    *(int *)pageHandle = schema->keySize; // Key Size
    pageHandle += sizeof(int);

    // Loop through attributes
    for (int k = 0; k < schema->numAttr; k++)
    {
        strncpy(pageHandle, schema->attrNames[k], ATTRIBUTE_SIZE); // Attribute name
        pageHandle += ATTRIBUTE_SIZE;

        *(int *)pageHandle = (int)schema->dataTypes[k]; // Data type
        pageHandle += sizeof(int);

        *(int *)pageHandle = (int)schema->typeLength[k]; // Type length
        pageHandle += sizeof(int);
    }

    SM_FileHandle fileHandle;

    // Create a page file with the table name using the storage manager
    int result;
    if ((result = createPageFile(name)) != RC_OK)
    {
        free(recordManager);
        return result;
    }

    // Open the newly created page file
    if ((result = openPageFile(name, &fileHandle)) != RC_OK)
    {
        free(recordManager);
        return result;
    }

    // Write the schema to the first block of the page file
    if ((result = writeBlock(0, &fileHandle, data)) != RC_OK)
    {
        free(recordManager);
        return result;
    }

    // Close the file after writing
    if ((result = closePageFile(&fileHandle)) != RC_OK)
    {
        free(recordManager);
        return result;
    }

    return RC_OK;
}

// This function opens the table with table name "name"
extern RC openTable(RM_TableData *rel, char *name)
{
    SM_PageHandle pageHandle;

    // Setting table's metadata to our custom record manager metadata structure
    rel->mgmtData = recordManager;
    // Setting the table's name
    rel->name = name;

    // Pinning a page, putting a page in the Buffer Pool using Buffer Manager
    if (pinPage(&recordManager->bufferPool, &recordManager->pageHandle, 0) != RC_OK)
    {
        return RC_PIN_PAGE_FAILED;
    }

    // Setting the initial pointer (0th location) to the record manager's page data
    pageHandle = (char *)recordManager->pageHandle.data;

    // Retrieving total number of tuples from the page file
    recordManager->tuplesCount = *(int *)pageHandle;
    pageHandle += sizeof(int);

    // Getting free page from the page file
    recordManager->freePage = *(int *)pageHandle;
    pageHandle += sizeof(int);

    // Getting the number of attributes from the page file
    int attributeCount = *(int *)pageHandle;
    pageHandle += sizeof(int);

    // Allocate memory space for 'schema'
    Schema *schema = (Schema *)malloc(sizeof(Schema));

    // Setting schema's parameters using designated initializers
    *schema = (Schema){
        .numAttr = attributeCount,
        .attrNames = (char **)malloc(sizeof(char *) * attributeCount),
        .dataTypes = (DataType *)malloc(sizeof(DataType) * attributeCount),
        .typeLength = (int *)malloc(sizeof(int) * attributeCount),
    };

    // Allocate memory space for storing attribute name for each attribute
    for (int k = 0; k < attributeCount; k++)
    {
        schema->attrNames[k] = (char *)malloc(ATTRIBUTE_SIZE);
    }

    for (int k = 0; k < schema->numAttr; k++)
    {
        // Setting attribute name
        strncpy(schema->attrNames[k], pageHandle, ATTRIBUTE_SIZE);
        pageHandle += ATTRIBUTE_SIZE;

        // Setting data type of attribute
        schema->dataTypes[k] = *(int *)pageHandle;
        pageHandle += sizeof(int);

        // Setting length of datatype (length of STRING) of the attribute
        schema->typeLength[k] = *(int *)pageHandle;
        pageHandle += sizeof(int);
    }

    // Setting the newly created schema to the table's schema
    rel->schema = schema;

    // Unpinning the page, removing it from the Buffer Pool using Buffer Manager
    if (unpinPage(&recordManager->bufferPool, &recordManager->pageHandle) != RC_OK)
    {
        freeSchema(schema);
        return RC_UNPIN_PAGE_FAILED;
    }

    // Write the page back to disk using Buffer Manager
    if (forcePage(&recordManager->bufferPool, &recordManager->pageHandle) != RC_OK)
    {
        freeSchema(schema);
        return RC_FORCE_PAGE_FAILED;
    }

    return RC_OK;
}

// This function closes the table referenced by "rel"

extern RC closeTable(RM_TableData *rel)
{
    // Check if the relation is already closed
    if (rel == NULL || rel->mgmtData == NULL)
    {
        return RC_OK; // Already closed, nothing to do
    }

    rel->mgmtData = NULL; // set relation to NULL to close

    return RC_OK;
}

// This function returns the number of tuples (records) in the table referenced by "rel"
extern int getNumTuples(RM_TableData *rel)
{
    // Accessing our data structure's tuplesCount and returning it
    RecordManager *recordManager;
    int tuplesCount;

    recordManager = rel->mgmtData;
    tuplesCount = recordManager->tuplesCount;

    return tuplesCount;
}

// This function deletes the table having table name "name"
extern RC deleteTable(char *name)
{
    RC result = destroyPageFile(name);

    int deletionFailed = (result != RC_OK);
    // Removing the page file from memory using the storage manager
    if (deletionFailed)
    {
        return RC_DELETE_TABLE_FAILED;
    }

    return RC_OK;
}

// ******** RECORD FUNCTIONS ******** //

// This function inserts a new record in the table referenced by "rel" and updates the 'record' parameter with the Record ID of he newly inserted record
extern RC insertRecord(RM_TableData *rel, Record *record)
{
    int recordSize;
    RecordManager *recordManager;
    char *data, *slotPointer;
    RID *recordID;

    recordManager = rel->mgmtData;

    recordID = &record->id;

    recordSize = getRecordSize(rel->schema);

    recordID->page = recordManager->freePage;

    BM_BufferPool *bufferPool = &recordManager->bufferPool;
    BM_PageHandle *pageHandle = &recordManager->pageHandle;

    pinPage(bufferPool, pageHandle, recordID->page);

    data = recordManager->pageHandle.data;

    recordID->slot = findFreeSlot(data, recordSize);

    int newPage;

    while (recordID->slot == -1)
    {
        unpinPage(bufferPool, pageHandle);
        // recordID->page++;
        newPage = recordID->page + 1;
        recordID->page = newPage;
        pinPage(bufferPool, pageHandle, recordID->page);

        data = recordManager->pageHandle.data;
        recordID->slot = findFreeSlot(data, recordSize);
    }

    int slotOffset;

    slotPointer = data;
    markDirty(bufferPool, pageHandle);

    slotOffset = recordID->slot * recordSize;
    slotPointer = slotPointer + slotOffset;

    *slotPointer = '+';

    /*

    if (++slotPointer && record->data + 1 && recordSize - 1)
    {
        memcpy(slotPointer, record->data + 1, recordSize - 1);
    }
    */

    // Define new variables
    char *dataStartPosition = record->data + 1;
    int remainingRecordSize = recordSize - 1;

    if (++slotPointer && dataStartPosition && remainingRecordSize)
    {
        memcpy(slotPointer, dataStartPosition, remainingRecordSize);
    }

    unpinPage(bufferPool, pageHandle);
    recordManager->tuplesCount++;
    pinPage(bufferPool, pageHandle, 0);

    return RC_OK;
}

// This function updates a record referenced by "record" in the table referenced by "rel"
extern RC updateRecord(RM_TableData *rel, Record *record)
{
    // Retrieve metadata from the table
    RecordManager *recordManager;
    recordManager = rel->mgmtData;

    // Pin the page containing the record to be updated
    // pinPage(&recordManager->bufferPool, &recordManager->pageHandle, record->id.page);
    BM_BufferPool *buffer = &recordManager->bufferPool;
    BM_PageHandle *pageHandle = &recordManager->pageHandle;
    int page = record->id.page;

    pinPage(buffer, pageHandle, page);

    // Calculate the offset within the page for the target record
    int recordSize = getRecordSize(rel->schema);
    int offset = record->id.slot * recordSize;

    // Update the tombstone and copy the new record data
    char *pageData = recordManager->pageHandle.data;
    pageData[offset] = '+';
    // memcpy(pageData + offset + 1, record->data + 1, recordSize - 1);
    //  Calculate the source and destination pointers and sizes
    char *sourcePointer = record->data + 1;
    char *destinationPointer = pageData + offset + 1;
    int sizeToCopy = recordSize - 1;

    // Perform the memory copy using new variables
    memcpy(destinationPointer, sourcePointer, sizeToCopy);

    // Mark the page as dirty since it has been modified
    markDirty(buffer, pageHandle);

    // Unpin the page after the update
    unpinPage(buffer, pageHandle);

    return RC_OK;
}

// This function deletes a record having Record ID "id" in the table referenced by "rel"
extern RC deleteRecord(RM_TableData *rel, RID id)
{
    char *data;
    RecordManager *recordManager;
    recordManager = rel->mgmtData;
    int recordSize;

    // pinPage(&recordManager->bufferPool, &recordManager->pageHandle, id.page);

    BM_BufferPool *bufferPool = &recordManager->bufferPool;
    BM_PageHandle *pageHandle = &recordManager->pageHandle;

    pinPage(bufferPool, pageHandle, id.page);

    recordManager->freePage = id.page;

    data = recordManager->pageHandle.data;

    recordSize = getRecordSize(rel->schema);

    int slot = 0;
    while (slot < id.slot)
    {
        data += recordSize;
        slot++;
    }

    if (*data != '\0')
    {
        *data = '-';
        markDirty(bufferPool, pageHandle);
    }

    unpinPage(bufferPool, pageHandle);

    return RC_OK;
}

// This function retrieves a record having Record ID "id" in the table referenced by "rel".
// The result record is stored in the location referenced by "record"
extern RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    // Retrieving our meta data stored in the table
    RecordManager *recordManager;

    recordManager = rel->mgmtData;

    // Pinning the page which has the record we want to retrieve
    // pinPage(&recordManager->bufferPool, &recordManager->pageHandle, id.page);
    BM_BufferPool *bufferPool = &recordManager->bufferPool;
    BM_PageHandle *pageHandle = &recordManager->pageHandle;
    int pageNumber = id.page;

    pinPage(bufferPool, pageHandle, pageNumber);

    // Getting the size of the record
    char *dataPointer;
    int recordSize;

    recordSize = getRecordSize(rel->schema);

    char *data;

    dataPointer = recordManager->pageHandle.data;

    // dataPointer += id.slot * recordSize;
    size_t offset = id.slot * recordSize;
    dataPointer += offset;

    while (*dataPointer != '+')
    {
        // Return error if no matching record for Record ID 'id' is found in the table
        unpinPage(bufferPool, pageHandle);
        return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
    }

    // Setting the Record ID
    record->id = id;

    // Setting the pointer to the data field of 'record' so that we can copy the data of the record

    data = record->data;

    // Copy data using C's function memcpy(...)

    char *nextData = dataPointer + 1;
    int remainingRecordSize = recordSize - 1;
    if (*dataPointer == '+')
    {
        memcpy(++data, nextData, remainingRecordSize);
    }

    int rc = RC_OK;

    // Unpin the page after the record is retrieved since the page is no longer required to be in memory
    unpinPage(bufferPool, pageHandle);

    return rc;
}
// ******** SCAN FUNCTIONS ******** //

// This function scans all the records using the condition
extern RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    // Checking if scan condition (test expression) is present
    int isConditionNull = (cond == NULL);
    int conditionNotFound = RC_SCAN_CONDITION_NOT_FOUND;
    RecordManager *tableManager;

    if (isConditionNull)
    {
        return conditionNotFound;
    }
    RecordManager *scanManager;

    // Open the table in memory
    openTable(rel, "ScanTable");

    // Allocating memory to the scanManager
    size_t sizeOfRecordManager = sizeof(RecordManager);
    scanManager = (RecordManager *)malloc(sizeOfRecordManager);

    int startPage, startSlot, startScanCount;

    // Setting the scan's meta data to our meta data
    scan->mgmtData = scanManager;
    // scan->mgmtData = (RecordManager *)malloc(sizeOfRecordManager);

    // Initializing variables for scan start
    startPage = 1;
    startSlot = 0;
    startScanCount = 0;

    // Setting initial values for scan
    scanManager->recordID.page = startPage;
    scanManager->recordID.slot = startSlot;
    scanManager->scanCount = startScanCount;

    // Setting our meta data to the table's meta data
    tableManager = rel->mgmtData;

    // Setting the tuple count
    tableManager->tuplesCount = ATTRIBUTE_SIZE;

    // Setting the scan condition
    scanManager->condition = cond;

    // Setting the scan's table, i.e., the table which has to be scanned using the specified condition
    scan->rel = rel;

    return RC_OK;
}

// This function scans each record in the table and stores the result record (record satisfying the condition)
// in the location pointed by  'record'.
/*extern RC next(RM_ScanHandle *scan, Record *record)
{
    RecordManager *tableManager;
    RecordManager *scanManager;
    Schema *schema;

    scanManager = scan->mgmtData;

    int rc = RC_SCAN_CONDITION_NOT_FOUND;

    tableManager = scan->rel->mgmtData;

    schema = scan->rel->schema;

    if (scanManager->condition == NULL)
    {
        return RC_SCAN_CONDITION_NOT_FOUND;
    }

    Value *result = (Value *)malloc(sizeof(Value));
    char *data;

    int recordSize = getRecordSize(schema);
    int totalSlots = PAGE_SIZE / recordSize;
    int tuplesCount = tableManager->tuplesCount;

    if (tuplesCount == 0)
    {
        return RC_RM_NO_MORE_TUPLES;
    }

    int scanCount = scanManager->scanCount;
    while (scanCount <= tuplesCount)
    {
        if (scanCount <= 0)
        {
            scanManager->recordID.page = 1;
            scanManager->recordID.slot = 0;
        }
        else
        {
            scanManager->recordID.slot++;
            if (scanManager->recordID.slot >= totalSlots)
            {
                scanManager->recordID.slot = 0;
                scanManager->recordID.page++;
            }
        }

        pinPage(&tableManager->bufferPool, &scanManager->pageHandle, scanManager->recordID.page);
        data = scanManager->pageHandle.data;
        data = data + (scanManager->recordID.slot * recordSize);

        record->id.page = scanManager->recordID.page;
        record->id.slot = scanManager->recordID.slot;

        char *dataPointer = record->data;
        *dataPointer = '-';
        memcpy(++dataPointer, data + 1, recordSize - 1);

        scanManager->scanCount++;
        scanCount++;

        evalExpr(record, schema, scanManager->condition, &result);

        if (result->v.boolV == TRUE)
        {
            unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);
            return RC_OK;
        }
    }

    unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);
    scanManager->recordID.page = 1;
    scanManager->recordID.slot = 0;
    scanManager->scanCount = 0;

    return RC_RM_NO_MORE_TUPLES;
}*/

extern RC next(RM_ScanHandle *scan, Record *record)
{
    RecordManager *tableManager;
    RecordManager *scanManager;
    Schema *schema;

    scanManager = scan->mgmtData;

    int rc = RC_SCAN_CONDITION_NOT_FOUND;

    tableManager = scan->rel->mgmtData;

    int isConditionNull = (scanManager->condition == NULL);

    schema = scan->rel->schema;

    if (isConditionNull)
    {
        return rc;
    }
    char *data;
    Value *result;

    int recordSize, totalSlots, tuplesCount;

    size_t valueSize = sizeof(Value);

    result = (Value *)malloc(valueSize);

    recordSize = getRecordSize(schema);
    totalSlots = PAGE_SIZE / recordSize;
    int scanCount;
    tuplesCount = tableManager->tuplesCount;

    int isTuplesCountZero = (tuplesCount == 0);

    if (isTuplesCountZero)
    {
        return RC_RM_NO_MORE_TUPLES;
    }

    scanCount = scanManager->scanCount;

    while (scanCount <= tuplesCount)
    {
        int currentPage = scanManager->recordID.page;
        int currentSlot = scanManager->recordID.slot;

        if (scanCount <= 0)
        {
            currentPage = 1;
            currentSlot = 0;
        }
        else
        {
            currentSlot++;
            if (currentSlot >= totalSlots)
            {
                currentSlot = 0;
                currentPage++;
            }
        }

        scanManager->recordID.page = currentPage;
        scanManager->recordID.slot = currentSlot;

        BM_BufferPool *bufferPoolRef = &tableManager->bufferPool;
        BM_PageHandle *pageHandleRef = &scanManager->pageHandle;
        int *recordIDPageRef = &scanManager->recordID.page;

        pinPage(bufferPoolRef, pageHandleRef, *recordIDPageRef);

        char *dataPointer;

        data = scanManager->pageHandle.data + (scanManager->recordID.slot * recordSize);

        int newPage = scanManager->recordID.page;
        int newSlot = scanManager->recordID.slot;

        record->id.page = newPage;
        record->id.slot = newSlot;

        dataPointer = record->data;

        char *newDataPointer = data + 1;
        int newSize = recordSize - 1;

        *dataPointer = '-';
        memcpy(++dataPointer, newDataPointer, newSize);

        scanManager->scanCount++;
        scanCount++;

        Expr *condition = scanManager->condition;
        evalExpr(record, schema, condition, &result);

        int isResultTrue = (result->v.boolV == TRUE);

        BM_BufferPool *newBufferPool = &tableManager->bufferPool;

        int rc = RC_OK;

        if (isResultTrue)
        {
            unpinPage(newBufferPool, &scanManager->pageHandle);
            return rc;
        }
    }

    BM_BufferPool *bufferPool = &tableManager->bufferPool;
    BM_PageHandle *pageHandle = &scanManager->pageHandle;
    RID *recordID = &scanManager->recordID;

    unpinPage(bufferPool, pageHandle);
    recordID->page = 1;
    recordID->slot = 0;
    scanManager->scanCount = 0;

    return RC_RM_NO_MORE_TUPLES;
}

// This function closes the scan operation.

extern RC closeScan(RM_ScanHandle *scan)
{
    RecordManager *scanManager;
    scanManager = scan->mgmtData;
    RecordManager *recordManager;
    recordManager = scan->rel->mgmtData;

    // Check if scan was incomplete
    while (scanManager->scanCount > 0)
    {
        // Unpin the page i.e. remove it from the buffer pool.
        BM_BufferPool *buffer = &recordManager->bufferPool;
        BM_PageHandle *pageHandle = &scanManager->pageHandle;
        unpinPage(buffer, pageHandle);

        // Reset the Scan Manager's values
        scanManager->scanCount = 0;
        RID *recordID = &scanManager->recordID;
        recordID->page = 1;
        recordID->slot = 0;
    }

    // De-allocate all the memory space allocated to the scans's meta data (our custom structure)
    if (scan->mgmtData != NULL)
    {
        free(scan->mgmtData);
        scan->mgmtData = NULL;
    }

    return RC_OK;
}

// ******** SCHEMA FUNCTIONS ******** //
extern Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    int i = 0;
    size_t schemaSize;
    Schema *schema;
    schemaSize = sizeof(Schema);
    schema = (Schema *)malloc(schemaSize); // Allocate memory space for schema

    bool allocationSuccessful = (schema != NULL);

    if (allocationSuccessful)
    {
        *schema = (Schema){
            .dataTypes = dataTypes,   // Set the Data Type of the Attributes in the new schema
            .keyAttrs = keys,         // Set the Key Attributes in the new schema
            .attrNames = attrNames,   // Set the Attribute Names in the new schema
            .numAttr = numAttr,       // Set the Number of Attributes in the new schema
            .typeLength = typeLength, // Set the Type Length of the Attributes in the new schema
            .keySize = keySize        // Set the Key Size in the new schema
        };
    }

    return schema;
}

// This function returns the record size of the schema referenced by "schema"

extern int getRecordSize(Schema *schema)
{
    int numberOfAttributes = schema->numAttr;

    int i = 0; // Initialize counter

    int size = 0;
    // Process each attribute in the schema using a while loop
    // while (i < schema->numAttr)
    while (i < numberOfAttributes)
    {
        DataType currentDataType = schema->dataTypes[i];

        // Check data type using if statements
        if (currentDataType == DT_STRING)
        {
            size += schema->typeLength[i]; // Add type length for string
        }
        else if (currentDataType == DT_INT)
        {
            size += sizeof(int); // Add size of int
        }
        else if (currentDataType == DT_FLOAT)
        {
            size += sizeof(float); // Add size of float
        }
        else if (currentDataType == DT_BOOL)
        {
            size += sizeof(bool); // Add size of bool
        }

        i++; // Increment counter
    }

    // return ++size; // Increment size and return
    int newSize = ++size;
    return newSize;
}

// This function removes a schema from memory and de-allocates all the memory space allocated to the schema.
extern RC freeSchema(Schema *schema)
{
    bool allocationSuccessful = (schema != NULL);
    // De-allocating memory space occupied by 'schema'
    if (allocationSuccessful)
    {
        free(schema);
        return RC_OK;
    }
    return RC_ERROR; // Indicate an error if the schema is already NULL
}

// ******** DEALING WITH RECORDS AND ATTRIBUTE VALUES ******** //

// This function creates a new record in the schema referenced by "schema"
extern RC createRecord(Record **record, Schema *schema)
{
    // Allocate memory space for the new record
    Record *newRecord;
    int recordSize;
    size_t record_Size = sizeof(Record);
    newRecord = (Record *)malloc(record_Size);

    if (newRecord == NULL)
    {
        return RC_RM_NO_MORE_TUPLES;
    }

    // Retrieve the record size

    recordSize = getRecordSize(schema);

    // Allocate memory space for the data of the new record
    newRecord->data = (char *)malloc(recordSize);
    if (newRecord->data == NULL)
    {
        free(newRecord);
        return RC_RM_NO_MORE_TUPLES;
    }

    // Setting page and slot position to -1 because this is a new record
    newRecord->id.page = newRecord->id.slot = -1;

    // Getting the starting position in memory of the record's data
    char *dataPointer = newRecord->data;

    // Use '-' for Tombstone mechanism, set it to '-' because the record is empty
    *dataPointer = '-';

    // Move the position by one before adding NULL
    dataPointer++;

    // Append '\0' which means NULL in C to the record after tombstone
    *dataPointer = '\0';

    // Set the newly created record to 'record' which is passed as an argument
    *record = newRecord;

    return RC_OK;
}

// This function sets the offset (in bytes) from initial position to the specified attribute of the record into the 'result' parameter passed through the function

RC attrOffset(Schema *schema, int attrNum, int *result)
{
    *result = 1; // Start from offset 0

    for (int i = 0; i < attrNum; i++)
    {
        // Map data types to their respective sizes
        int typeSizes[] = {[DT_STRING] = sizeof(int), [DT_INT] = sizeof(int), [DT_FLOAT] = sizeof(float), [DT_BOOL] = sizeof(bool)};

        // Add the size of each attribute based on its data type
        *result += schema->dataTypes[i] == DT_STRING
                       ? schema->typeLength[i]
                       : typeSizes[schema->dataTypes[i]];
    }

    return RC_OK;
}
// This function removes the record from the memory.
extern RC freeRecord(Record *record)
{
    bool isRecordNotNull = (record != NULL);
    // Check if the record pointer is not NULL before freeing
    if (isRecordNotNull)
    {
        // Deallocate memory space allocated to record and free up that space
        free(record);
        return RC_OK;
    }
    else
    {
        // Return an error code indicating that the pointer was already NULL
        return RC_NULL_POINTER;
    }
}

// This function retrieves an attribute from the given record in the specified schema
extern RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
    int offset;
    offset = 0;

    // Getting the offset value of attributes depending on the attribute number
    attrOffset(schema, attrNum, &offset);

    // Allocating memory space for the Value data structure where the attribute values will be stored
    Value *attribute;
    size_t valueSize = sizeof(Value);

    char *dataPointer;

    attribute = (Value *)malloc(valueSize);

    // Getting the starting position of record's data in memory

    dataPointer = record->data;

    // Adding offset to the starting position
    dataPointer += offset;

    // If attrNum = 1

    int isAttrNumOne = (attrNum == 1);
    if (isAttrNumOne)
    {
        schema->dataTypes[attrNum] = 1;
    }

    // Retrieve attribute's value depending on attribute's data type
    int dataType = schema->dataTypes[attrNum];
    if (dataType == DT_STRING)
    {
        int len;
        // Getting attribute value from an attribute of type STRING
        int length = schema->typeLength[attrNum];
        len = length + 1;
        attribute->v.stringV = (char *)malloc(len);

        // Copying string to location pointed by dataPointer and appending '\0' which denotes end of string in C

        char *additionalVariable = attribute->v.stringV;

        strncpy(additionalVariable, dataPointer, length);

        additionalVariable = '\0';
        attribute->dt = DT_STRING;
    }
    else if (dataType == DT_INT)
    {
        // Getting attribute value from an attribute of type INTEGER
        int value = 0;
        memcpy(&value, dataPointer, sizeof(int));
        attribute->v.intV = value;
        attribute->dt = DT_INT;
    }
    else if (dataType == DT_FLOAT)
    {
        // Getting attribute value from an attribute of type FLOAT
        float value;
        memcpy(&value, dataPointer, sizeof(float));
        attribute->v.floatV = value;
        attribute->dt = DT_FLOAT;
    }
    else if (dataType == DT_BOOL)
    {
        // Getting attribute value from an attribute of type BOOLEAN
        bool value;
        memcpy(&value, dataPointer, sizeof(bool));
        attribute->v.boolV = value;
        attribute->dt = DT_BOOL;
    }
    else
    {
        printf("Serializer not defined for the given datatype. \n");
    }

    int rc = RC_OK;

    *value = attribute;
    return rc;
}
// This function sets the attribute value in the record in the specified schema
extern RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
    char *dataPointer;
    int offset = 0;

    // Getting the offset value of attributes depending on the attribute number
    attrOffset(schema, attrNum, &offset);

    // Getting the starting position of record's data in memory

    dataPointer = record->data;

    // Adding offset to the starting position
    dataPointer += offset;

    DataType currentDataType = schema->dataTypes[attrNum];
    if (currentDataType == DT_STRING)
    {
        // Setting attribute value of an attribute of type STRING
        // Getting the length of the string as defined while creating the schema
        int length = schema->typeLength[attrNum];

        // Copying attribute's value to the location pointed by record's data (dataPointer)
        strncpy(dataPointer, value->v.stringV, length);
        dataPointer += length;
    }
    else if (currentDataType == DT_INT)
    {
        // Setting attribute value of an attribute of type INTEGER
        *(int *)dataPointer = value->v.intV;
        dataPointer += sizeof(int);
    }
    else if (currentDataType == DT_FLOAT)
    {
        // Setting attribute value of an attribute of type FLOAT
        *(float *)dataPointer = value->v.floatV;
        dataPointer += sizeof(float);
    }
    else if (currentDataType == DT_BOOL)
    {
        // Setting attribute value of an attribute of type BOOL
        *(bool *)dataPointer = value->v.boolV;
        dataPointer += sizeof(bool);
    }
    else
    {
        printf("Serializer not defined for the given datatype.\n");
    }

    return RC_OK;
}
