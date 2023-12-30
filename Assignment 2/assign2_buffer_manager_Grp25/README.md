# Buffer Manager

## How to run the code

To compile, build and run the test cases, use the following command

```bash
make run
```

## Solution Approach

## buffer_mgr.c

### Buffer pool functions

- **initBufferPool():**
    - Initializes a buffer pool with the given parameters.
    - Checks if the page file exists and opens it in read-write mode.
    - Sets the buffer pool properties including page file name, the number of pages, and the replacement strategy.
    - Allocates memory for management data (metadata of buffer pool).
    - Initializes page frames for LRU strategy, setting each frame with no page, NULL data, not dirty, and fix count to 0.
    - Returns RC_OK on successful initialization or an error code if unsuccessful.
- **shutdownBufferPool():**
    - Shuts down the buffer pool, flushing any dirty pages to disk.
    - Iterates through all pages and writes dirty pages to disk using the writePageToFile function.
    - Frees memory associated with page frames and management data.
    - Resets the management pointer in the buffer pool.
    - Returns RC_OK on successful shutdown or an error code if unsuccessful.
- **forceFlushPool():**
    - Forces a flush of all dirty pages with a fix count of 0 in the buffer pool.
    - Iterates through all pages and writes dirty pages to disk using the writePageToFile function.
    - Returns RC_OK on successful flush or an error code if unsuccessful.

### Page management functions

- **selectPageReplacementStrategy():**
    - A static function used to select the appropriate page replacement strategy based on the provided strategy enum.
    - Calls the corresponding page replacement function (FIFO, LRU, LRU_K) based on the strategy.
    - Returns the result of the selected strategy function.
- **pinPage():**
    - Main function to pin a page in the buffer pool based on the selected replacement strategy.
    - Checks for an invalid page number and calls selectPageReplacementStrategy to pin the page.
    - Returns the result of the page pinning operation.
- **findPageIndex():**
    - Helper function to find the index of a page in the buffer pool given its page number.
    - Used by other functions to locate a page within the buffer pool.
- **markDirty():**
    - Marks a page in the buffer pool as dirty.
    - Updates the isDirty flag for the specified page.
    - Returns RC_OK on success or an error code if unsuccessful.
- **unpinPage():**
    - Unpins a page in the buffer pool.
    - Decrements the fix count for the specified page.
    - Returns RC_OK on success or an error code if unsuccessful.
- **forcePage():**
    - Forces a page to be written to disk if it's dirty.
    - Calls writePageToFile to write the page and updates statistics.
    - Returns RC_OK on success or an error code if unsuccessful.

### Statistics functions

- **getFrameContents():**
    - Returns an array of page numbers corresponding to the pages stored in the buffer pool.
    - Allocates memory for the array and populates it.
    - Returns the array or an error code if the buffer pool doesn't exist.
- **getDirtyFlags():**
    - Returns an array of booleans indicating whether the pages in the buffer pool are dirty.
    - Allocates memory for the array and populates it.
    - Returns the array or an error code if the buffer pool doesn't exist.
- **getFixCounts():**
    - Returns an array of integers representing the fix count for each page in the buffer pool.
    - Allocates memory for the array and populates it.
    - Returns the array or an error code if the buffer pool doesn't exist.
- **getNumReadIO():**
    - Returns the number of read I/O operations performed on the buffer pool.
    - Returns the count or an error code if the buffer pool doesn't exist.
- **getNumWriteIO():**
    - Returns the number of write I/O operations performed on the buffer pool.
    - Returns the count or an error code if the buffer pool doesn't exist.

## buffer_mgr_helper.c

- **char *getPageFromFile():**
    - This function retrieves a page from a page file based on its page number. 
    - It opens the page file, allocates memory for the page data, reads the page into memory, and then closes the page file. 
    - It returns a pointer to the page data.

- **int findPage_LRU():**
    - This helper function finds a page in the buffer pool using the Least Recently Used (LRU) strategy. 
    - It returns the index of the page if found or -1 if not found.

- **int findLRUVictim():** 
    - This helper function finds a victim page in the buffer pool for replacement using the LRU strategy. 
    - It returns the index of the victim page if found or -1 if no victim is available.

- **void writePageToFile():** 
    - This function writes a page frame back to the page file. 
    - It opens the page file, writes the page data, and then closes the page file.

- **bool bufferPoolExists():**
    - This function checks if the buffer pool exists by verifying that the buffer pool and its management data are not NULL.

- **void updateLRUList():**
    - This function updates the LRU list of page frames, specifically the access count for a recently accessed frame.

- **int findPage_FIFO():**
    - This helper function finds a page in the buffer pool using the First-In-First-Out (FIFO) strategy. 
    - It returns the index of the page if found or -1 if not found.

- **int findVictimPage_FIFO():**
    - This helper function finds a victim page using the FIFO strategy. 
    - It returns the index of the victim page if found or -1 if no victim is available.

- **RC pinPageUsingFIFO():**
    - This function pins a page in the buffer pool using the FIFO replacement strategy.
    - It finds an available frame or a victim frame, reads the page into the frame, and updates management data.

- **RC pinPageUsingLRU():**
    - This function pins a page in the buffer pool using the LRU replacement strategy.
    - It finds an available frame or a victim frame, reads the page into the frame, updates management data, and maintains the LRU order.

- **RC pinPageUsingLRU_K():**
    - This function pins a page in the buffer pool using the LRU-K replacement strategy. 
    - It follows a similar process to pinPageUsingLRU but includes additional considerations for the K value.











