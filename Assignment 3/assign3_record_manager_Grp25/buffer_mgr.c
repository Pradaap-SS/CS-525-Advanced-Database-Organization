#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>
#include "buffer_mgr_helper.c"

// ***** BUFFER POOL FUNCTIONS ***** //

/*
   This function creates and initializes a buffer pool with numPages page frames.
   pageFileName stores the name of the page file whose pages are being cached in memory.
   strategy represents the page replacement strategy (FIFO, LRU, LFU, CLOCK) that will be used by this buffer pool
   stratData is used to pass parameters if any to the page replacement strategy
*/
extern RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                         const int numPages, ReplacementStrategy strategy,
                         void *stratData)
{
    PageFrame *page;
    bm->pageFile = (char *)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    int bufferSize, i = 0;

    // Reserve memory space = number of pages x space required for one page
    size_t pageFrameSize = sizeof(PageFrame) * numPages;
    page = malloc(pageFrameSize);

    // Buffer size is the total number of pages in memory or the buffer pool.

    bufferSize = numPages;

    // Initializing all pages in the buffer pool. The values of fields (variables) in the page are either NULL or 0

    while (i < bufferSize)
    {
        PageFrame *currentPage = &page[i];
        currentPage->data = NULL;
        currentPage->pageNum = -1;
        currentPage->dirtyBit = 0;
        currentPage->fixCount = 0;
        currentPage->hitNum = 0;
        currentPage->refNum = 0;
        ++i;
    }

    bm->mgmtData = page;
    return RC_OK;
}

// Function to shut down the buffer pool
extern RC shutdownBufferPool(BM_BufferPool *const bm)
{
    PageFrame *pageFrame;
    int i = 0;
    pageFrame = (PageFrame *)bm->mgmtData;

    // Write all dirty pages (modified pages) back to disk
    forceFlushPool(bm);

    int numPages = bm->numPages;

    while (i < numPages && pageFrame[i].fixCount == 0)
    {
        i++;
    }

    if (i < numPages)
    {
        return RC_PINNED_PAGES_IN_BUFFER;
    }

    // Release space occupied by the page
    free(pageFrame);
    bm->mgmtData = NULL;
    return RC_OK;
}

// Function to write all dirty pages (having fixCount = 0) to disk
extern RC forceFlushPool(BM_BufferPool *const bm)
{
    int i = 0;
    PageFrame *pageFrame;
    pageFrame = (PageFrame *)bm->mgmtData;

    // Store all dirty pages (modified pages) in memory to the page file on disk

    int fixCount, dirtyBit, pageNum;
    SM_FileHandle fh;
    char *data;

    while (i < bm->numPages)
    {
        fixCount = pageFrame[i].fixCount;
        dirtyBit = pageFrame[i].dirtyBit;
        pageNum = pageFrame[i].pageNum;
        data = pageFrame[i].data;

        if (fixCount == 0 && dirtyBit == 1)
        {
            SM_FileHandle fh;
            // Opening page file available on disk
            openPageFile(bm->pageFile, &fh);
            // Writing a block of data to the page file on disk
            writeBlock(pageNum, &fh, data);
            // Mark the page as not dirty.
            pageFrame[i].dirtyBit = 0;
        }
        i++;
    }
    return RC_OK;
}

// Function to update page replacement information
void updatePageReplacementInfo(BM_BufferPool *const bm, const int pageIndex)
{
    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;

    if (bm->strategy == RS_LRU)
    {
        // LRU algorithm uses the value of hit to determine the least recently used page
        pageFrame[pageIndex].hitNum = pageFrame[pageIndex].hitNum + 1;
    }
    else if (bm->strategy == RS_CLOCK)
    {
        // hitNum = 1 to indicate that this was the last page frame examined (added to the buffer pool)
        pageFrame[pageIndex].hitNum = 1;
    }
    else if (bm->strategy == RS_LFU)
    {
        // Incrementing refNum to add one more to the count of the number of times the page is used (referenced)
        pageFrame[pageIndex].refNum++;
    }
    // Add additional conditions for other replacement strategies if needed
}

// Function to apply page replacement strategy
void applyPageReplacementStrategy(BM_BufferPool *const bm, PageFrame *const newPage)
{
    if (bm->strategy == RS_FIFO)
    {
        FIFO(bm, newPage);
    }
    else if (bm->strategy == RS_LRU)
    {
        LRU(bm, newPage);
    }
    else if (bm->strategy == RS_CLOCK)
    {
        CLOCK(bm, newPage);
    }
    else if (bm->strategy == RS_LFU)
    {
        LFU(bm, newPage);
    }
    else if (bm->strategy == RS_LRU_K)
    {
        printf("\nLRU-k algorithm not implemented");
    }
    else
    {
        printf("\nAlgorithm Not Implemented\n");
    }
}
/*
// Function to pin a page with a page number pageNum
extern RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
                  const PageNumber pageNum)
{
    PageFrame *pageFrame;
    pageFrame = (PageFrame *)bm->mgmtData;

    int isFirstPageInvalid = (pageFrame[0].pageNum == -1);

    // Checking if the buffer pool is empty and this is the first page to be pinned
    if (isFirstPageInvalid)
    {
        // Reading the page from disk and initializing the page frame's content in the buffer pool
        SM_FileHandle fh;
        openPageFile(bm->pageFile, &fh);
        pageFrame[0].data = (SM_PageHandle)malloc(PAGE_SIZE);
        ensureCapacity(pageNum, &fh);
        readBlock(pageNum, &fh, pageFrame[0].data);
        pageFrame[0].pageNum = pageNum;
        pageFrame[0].fixCount++;
        pageFrame[0].hitNum = 0;
        pageFrame[0].refNum = 0;
        page->pageNum = pageNum;
        page->data = pageFrame[0].data;

        return RC_OK;
    }
    else
    {
        int i;
        bool isBufferFull = true;

        for (i = 0; i < bm->numPages; i++)
        {
            if (pageFrame[i].pageNum != -1)
            {
                // Checking if the page is in memory
                if (pageFrame[i].pageNum == pageNum)
                {
                    // Increasing fixCount, i.e., now there is one more client accessing this page
                    pageFrame[i].fixCount++;
                    isBufferFull = false;

                    // Incrementing hit (used by the LRU algorithm to determine the least recently used page)
                    pageFrame[i].hitNum++;

                    // Updating algorithm-specific values
                    updatePageReplacementInfo(bm, i);

                    page->pageNum = pageNum;
                    page->data = pageFrame[i].data;

                    break;
                }
            }
            else
            {
                SM_FileHandle fh;
                openPageFile(bm->pageFile, &fh);
                pageFrame[i].data = (SM_PageHandle)malloc(PAGE_SIZE);
                readBlock(pageNum, &fh, pageFrame[i].data);
                pageFrame[i].pageNum = pageNum;
                pageFrame[i].fixCount = 1;
                pageFrame[i].refNum = 0;

                // Incrementing hit (used by the LRU algorithm to determine the least recently used page)
                pageFrame[i].hitNum++;

                // Updating algorithm-specific values
                updatePageReplacementInfo(bm, i);

                page->pageNum = pageNum;
                page->data = pageFrame[i].data;

                isBufferFull = false;
                break;
            }
        }

        // If isBufferFull is true, then the buffer is full, and we must replace an existing page using the page replacement strategy
        if (isBufferFull)
        {
            // Create a new page to store data read from the file.
            PageFrame *newPage = (PageFrame *)malloc(sizeof(PageFrame));

            // Reading the page from disk and initializing the page frame's content in the buffer pool
            SM_FileHandle fh;
            openPageFile(bm->pageFile, &fh);
            newPage->data = (SM_PageHandle)malloc(PAGE_SIZE);
            readBlock(pageNum, &fh, newPage->data);
            newPage->pageNum = pageNum;
            newPage->dirtyBit = 0;
            newPage->fixCount = 1;
            newPage->refNum = 0;

            // Incrementing hit (used by the LRU algorithm to determine the least recently used page)
            newPage->hitNum++;

            // Updating algorithm-specific values
            updatePageReplacementInfo(bm, bm->numPages - 1);

            page->pageNum = pageNum;
            page->data = newPage->data;

            // Call the appropriate algorithm's function depending on the page replacement strategy selected (passed through parameters)
            applyPageReplacementStrategy(bm, newPage);
        }

        return RC_OK;
    }
}
*/

// Function to pin a page with a page number pageNum
extern RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
                  const PageNumber pageNum)
{
    PageFrame *pageFrame;
    pageFrame = (PageFrame *)bm->mgmtData;

    int isFirstPageInvalid = (pageFrame[0].pageNum == -1);

    // Checking if the buffer pool is empty and this is the first page to be pinned
    if (isFirstPageInvalid)
    {
        // Load the page from the disk and initialize the page frame's buffer pool content
        SM_FileHandle fh;

        openPageFile(bm->pageFile, &fh);

        pageFrame[0].data = (SM_PageHandle)malloc(PAGE_SIZE);
        char *dataPointer;
        ensureCapacity(pageNum, &fh);
        dataPointer = pageFrame[0].data;
        readBlock(pageNum, &fh, dataPointer);

        // Update the first page frame with new information
        pageFrame[0].fixCount++;
        pageFrame[0].refNum = 0;
        pageFrame[0].pageNum = pageNum;
        pageFrame[0].hitNum = 0;

        // Update the page itself with the new page number

        page->data = pageFrame[0].data;
        page->pageNum = pageNum;

        return RC_OK;
    }
    else
    {

        bool isBufferFull;
        isBufferFull = !false;
        int i;

        for (i = 0; i < bm->numPages; i++)
        {
            int isPageNumValid = (pageFrame[i].pageNum != -1);

            if (isPageNumValid)
            {
                int isPageNumEqual = (pageFrame[i].pageNum == pageNum);
                // Checking if the page is in memory
                if (isPageNumEqual)
                {
                    // Increasing fixCount, i.e., now there is one more client accessing this page
                    pageFrame[i].fixCount++;
                    isBufferFull = !true;

                    // Incrementing hit (used by the LRU algorithm to determine the least recently used page)
                    pageFrame[i].hitNum++;

                    // Updating algorithm-specific values
                    updatePageReplacementInfo(bm, i);

                    page->data = pageFrame[i].data;
                    page->pageNum = pageNum;

                    break;
                }
            }
            else
            {
                SM_FileHandle fh;
                openPageFile(bm->pageFile, &fh);
                pageFrame[i].data = (SM_PageHandle)malloc(PAGE_SIZE);
                char *dataPointer = pageFrame[i].data;
                readBlock(pageNum, &fh, dataPointer);

                // Update page frame information
                pageFrame[i].fixCount = 1;
                pageFrame[i].pageNum = pageNum;
                pageFrame[i].hitNum++;

                pageFrame[i].refNum = 0;

                // Updating algorithm-specific values
                updatePageReplacementInfo(bm, i);

                page->pageNum = pageNum;
                page->data = pageFrame[i].data;

                isBufferFull = !true;
                break;
            }
        }

        // If isBufferFull is true, then the buffer is full, and we must replace an existing page using the page replacement strategy
        if (isBufferFull)
        {
            // Create a new page to store data read from the file.
            PageFrame *newPage;
            // newPage = (PageFrame *)malloc(sizeof(PageFrame));
            size_t sizeOfPageFrame = sizeof(PageFrame);

            // Allocate memory for a new PageFrame
            newPage = (PageFrame *)malloc(sizeOfPageFrame);

            char *pageFile;

            // Reading the page from disk and initializing the page frame's content in the buffer pool
            SM_FileHandle fh;

            pageFile = bm->pageFile;
            openPageFile(pageFile, &fh);

            newPage->data = (SM_PageHandle)malloc(PAGE_SIZE);
            char *dataPtr = newPage->data;
            readBlock(pageNum, &fh, dataPtr);

            // Update new page information
            newPage->dirtyBit = 0;
            newPage->pageNum = pageNum;
            newPage->hitNum++;

            newPage->refNum = 0;
            newPage->fixCount = 1;

            // Incrementing hit (used by the LRU algorithm to determine the least recently used page)

            // Updating algorithm-specific values
            updatePageReplacementInfo(bm, bm->numPages - 1);

            page->pageNum = pageNum;
            page->data = newPage->data;

            // Call the appropriate algorithm's function depending on the page replacement strategy selected (passed through parameters)
            applyPageReplacementStrategy(bm, newPage);
        }

        return RC_OK;
    }
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