#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr_helper.c"

// Author: Ranjitha Aswath
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
{
    // File existence check and open
    FILE *fh = fopen(pageFileName, "rb+"); // open file in read write mode
    if (fh == NULL)                        // check if file is existing
    {
        if (pageFileName == NULL) // check if file name is exiting and open if it does
        {
            return RC_FILE_NOT_FOUND; // return error if not
        }
        return RC_FILE_NOT_FOUND; // return error if file doesnt exists
    }
    fclose(fh); // close the file if it exists

// Set buffer pool properties
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdiscarded-qualifiers"
    bm->pageFile = pageFileName; // set pagefile to buffer pool pointer
    bm->numPages = numPages;     // set numpages to buffer pool pointer
    bm->strategy = strategy;     // set strategy to buffer pool pointer
#pragma GCC diagnostic pop

    // Allocate memory for management data
    BM_MGMT_DATA *mgmtData;
    mgmtData = (BM_MGMT_DATA *)malloc(sizeof(BM_MGMT_DATA)); // memory allocation for mgmtdata
    if (mgmtData == NULL)                                    // check if memory allocation done
    {
        return RC_MEMORY_ALLOCATION_FAILED; // return error if unsuccesful
    }

    // Initializations to use FIFO strategy
    mgmtData->numReadIO = 0;  // initialize mgmtdata structure to numReadIO to 0
    mgmtData->numWriteIO = 0; // initialize mgmtdata structure to numWriteIO to 0
    mgmtData->queueHead = 0;  // initialize mgmtdata structute to queueHead to 0

    // Allocate memory for page frames
    mgmtData->frames = (PAGE_FRAME *)calloc(numPages, sizeof(PAGE_FRAME)); // memory allocation for page frame
    if (mgmtData->frames == NULL)                                          // check if memory allocation is complete
    {
        free(mgmtData);                     // free previously allocated memory
        return RC_MEMORY_ALLOCATION_FAILED; // return error if memory allocation is not succesful
    }

    // Initialize page frames for LRU strategy
    // set the page number to NO_PAGE, data to NULL, marks the frame as not dirty, and sets the fix count to 0.
    // initialize the recentAccessCount to 0,
    for (int i = 0; i < numPages; i++)
    {
        mgmtData->frames[i] = (PAGE_FRAME){NO_PAGE, NULL, false, 0, 0};
    }

    // Update mgmtData pointer in the buffer pool
    bm->mgmtData = mgmtData;

    return RC_OK; // return success message if bufferpool is initialized
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
    if (!bufferPoolExists(bm)) // check if buffer pool is existing
    {
        return RC_BUFFER_POOL_NOT_EXISTING; // return error if not
    }

    BM_MGMT_DATA *mgmtData;
    mgmtData = (BM_MGMT_DATA *)bm->mgmtData; // define mgmt data
    int i = 0;

    // run iteration to flush out dirty pages to disk before shutting down buffer pool
    while (i < bm->numPages)
    {
        PAGE_FRAME *frame = &mgmtData->frames[i];
        if (frame->isDirty && frame->fixCount == 0) // checking condition if frame is dirty and fix count is 0
        {
            // if true write to file by calling writepagetofile function
            writePageToFile(bm, frame);
            // increment the numwritecount
            mgmtData->numWriteIO++;
        }
        free(frame->data); // free dataframe
        i++;               // Increment the index
    }

    free(mgmtData->frames); // free frame to ensure no memory leak
    free(mgmtData);         // free mgmtdata to ensure no memory leak

    // reset the management pointer to null
    bm->mgmtData = NULL;

    return RC_OK; // return ok if successfully shutdown bufferpool
}
RC forceFlushPool(BM_BufferPool *const bm)
{
    // check conditions if buffer pool is existing
    if (!bufferPoolExists(bm))
    {
        return RC_BUFFER_POOL_NOT_EXISTING; // return error if not existing
    }

    BM_MGMT_DATA *mgmtData;
    mgmtData = (BM_MGMT_DATA *)bm->mgmtData; // defining mgmtdata

    // Perform a forced flush operation for all dirty pages with fix count 0 in the buffer pool

    int i = 0;
    // perform forced flush operation in while loop
    while (i < bm->numPages)
    {
        PAGE_FRAME *frame = &mgmtData->frames[i]; // set page frame to mgmt frame pointer
        // checking conditions if frame is dirty and fixing the count to 0
        if (frame->isDirty && frame->fixCount == 0)
        {
            writePageToFile(bm, frame); // write dirty page to disk
            mgmtData->numWriteIO++;     // increment write IO
            frame->isDirty = false;     // make page not dirty after writing back to disk
        }
        i++; // Increment the index
    }

    return RC_OK;
}
// Function to select the page replacement strategy based on the provided strategy enum
static RC selectPageReplacementStrategy(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    RC result = RC_OK;
    // strategy selection
    if (bm->strategy == RS_FIFO) // check if strategy is FIFO
    {
        result = pinPageUsingFIFO(bm, page, pageNum); // call pinPageUsingFifo
    }
    else if (bm->strategy == RS_LRU) // check if strategy is LRU
    {
        result = pinPageUsingLRU(bm, page, pageNum); // call pinPageUsingLRU
    }
    else if (bm->strategy == RS_LRU_K) // check if strategy is LRU_K
    {
        result = pinPageUsingLRU_K(bm, page, pageNum); // call pinPageUsingLRU_K
    }
    else
    {
        result = RC_INVALID_REPLACEMENT_STRATEGY; // return error if no strategy is met
    }

    return result; // return the result of strategy
}

// Main pinPage function
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    RC result;
    // Check for invalid page number
    if (pageNum < 0)
    {
        result = RC_READ_NON_EXISTING_PAGE;
        return result;
    }

    // Call the strategy selection function and return its result
    return selectPageReplacementStrategy(bm, page, pageNum);
}

// Author: Pradaap Shiva Kumar Shobha

// Helper function to find a page's index in the buffer pool
int findPageIndex(BM_MGMT_DATA *mgmtData, int targetPageNum, BM_BufferPool *const bm)
{
    // Pointer to the array of PAGE_FRAME
    PAGE_FRAME *frames = mgmtData->frames;
    // Total number of pages in the buffer manager
    int numPages = bm->numPages;

    for (int i = 0; i < numPages; i++)
    {
        // Checking if the current page's pageNum matching the targetPageNum
        if (frames[i].pageNum == targetPageNum)
        {
            // If a match is found, return the index of the page in the buffer
            return i; // Page found, return its index
        }
    }

    return -1; // Page not found
}

RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // Check if the buffer pool exists
    if (!bufferPoolExists(bm))
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }
    // Get the mgmtData pointer from the buffer pool
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)bm->mgmtData;
    // Find the index of the frame that contains the page with the specified page number.
    int frameIndex = findPageIndex(mgmtData, page->pageNum, bm);

    // If the frame index is -1, it means the page does not exist in the buffer pool.
    if (frameIndex == -1)
    {
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Mark the page as dirty
    mgmtData->frames[frameIndex].isDirty = true;

    return RC_OK;
}

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // Check if the buffer pool exists
    if (!bufferPoolExists(bm))
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }

    // Get the mgmtData pointer from the buffer pool
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)bm->mgmtData;
    // Find the index of the frame that contains the page with the specified page number.
    int frameIndex = findPageIndex(mgmtData, page->pageNum, bm);

    // If the frameIndex is -1, it means the page doesn't exist in the buffer pool.
    if (frameIndex == -1)
    {
        return RC_READ_NON_EXISTING_PAGE;
    }
    // Check if the page is currently pinned (fixCount > 0)
    // if yes, decrement the fixCount.
    if (mgmtData->frames[frameIndex].fixCount > 0)
    {
        mgmtData->frames[frameIndex].fixCount--;
    }

    return RC_OK;
}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // Check if the buffer pool exists in the Buffer Manager.
    if (!bufferPoolExists(bm))
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }
    // Get the mgmtData pointer from the buffer pool
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)bm->mgmtData;
    // Find the index of the frame that contains the page with the specified page number.
    int frameIndex = findPageIndex(mgmtData, page->pageNum, bm);
    // If the frameIndex is -1, it means the page doesn't exist in the buffer pool.
    if (frameIndex == -1)
    {
        return RC_READ_NON_EXISTING_PAGE;
    }
    // Get a reference to the frame in which the page is stored.
    PAGE_FRAME *frame = &mgmtData->frames[frameIndex];
    // Write the page back to disk
    writePageToFile(bm, frame);
    // Increment the count of write I/O operations.
    mgmtData->numWriteIO++;
    // Mark the page as not dirty after it has been written back to disk
    frame->isDirty = false;

    return RC_OK;
}

PageNumber *getFrameContents(BM_BufferPool *const bm)
{
    // Check if the buffer pool exists in the Buffer Manager.
    if (!bufferPoolExists(bm))
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }
    // Get the mgmtData pointer from the buffer pool
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)bm->mgmtData;
    // Total number of pages in the buffer manager
    int numPages = bm->numPages;
    // Allocate memory for an array of PageNumber to store page numbers
    PageNumber *pageNumbers = malloc(numPages * sizeof(PageNumber));

    for (int i = 0; i < numPages; i++)
    {
        // Array stores the pagenumber
        pageNumbers[i] = mgmtData->frames[i].pageNum;
    }

    return pageNumbers;
}

// Author: Geeta Sudhakar Matkar

// this function will return array of bools whose ith element is true if page stored in ith frame is dirty else false

bool *getDirtyFlags(BM_BufferPool *const bm)
{
    // Check if the buffer pool exists in the Buffer Manager.
    if (!bufferPoolExists(bm))
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }
    //   store bufferpool's data in mgmtData
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)bm->mgmtData;

    //   numpages stores number of pages
    int numPages = bm->numPages;

    //   boolean array whose size is the size of bool times number of pages
    bool *dirtyFlags = (bool *)malloc(numPages * sizeof(bool));

    // Handle memory allocation failure
    if (dirtyFlags == NULL)
    {
        return NULL;
    }

    // frames of type PAGE_FRAME to store the frames from the buffer pool
    PAGE_FRAME *frames;
    frames = mgmtData->frames;

    // Iterating through the frames to populate dirtyFlags
    for (int i = 0; i < numPages; i++)
    {
        dirtyFlags[i] = frames[i].isDirty;
    }

    return dirtyFlags;
}

// this function will return an array of ints whose ith element is the fix count of the page stored in ith page frame

int *getFixCounts(BM_BufferPool *const bm)
{
    // Check if the buffer pool exists

    if (!bufferPoolExists(bm))
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }

    //   store bufferpool's data in mgmtData
    BM_MGMT_DATA *mgmtData;
    mgmtData = (BM_MGMT_DATA *)bm->mgmtData;

    //  numPages stores number of pages
    int numPages = bm->numPages;

    // frames of type PAGE_FRAME to store the frames from the buffer pool
    PAGE_FRAME *frames = mgmtData->frames;

    // integer array to store the fixcounts
    int *fixCounts = (int *)malloc(numPages * sizeof(int));

    // Handle memory allocation failure here if needed
    if (fixCounts == NULL)
    {
        return NULL;
    }

    // for loop iterates numPages times
    // copy the fix counts from the frames into the fixCounts array
    for (int i = 0; i < numPages; i++)
    {
        fixCounts[i] = frames[i].fixCount;
    }

    return fixCounts;
}

int getNumReadIO(BM_BufferPool *const bm)
{
    // Check if the buffer pool exists

    if (!bufferPoolExists(bm))
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }

    //  get a pointer to the management data of the buffer pool.
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)bm->mgmtData;

    // store the number of read I/O operations from the management data.
    int numReadIO = mgmtData->numReadIO;

    return numReadIO;
}

int getNumWriteIO(BM_BufferPool *const bm)
{
    // Check if the buffer pool exists
    if (!bufferPoolExists(bm))
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }

    //   get a pointer to the management data of the buffer pool.
    BM_MGMT_DATA *mgmtData = (BM_MGMT_DATA *)bm->mgmtData;

    //  store the number of write I/O operations from the management data.
    int numWriteIO = mgmtData->numWriteIO;

    return numWriteIO;
}
