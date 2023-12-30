/*helper functions for buffer pool, manager and replacement strategies implementation*/
#include <limits.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "dberror.h"
#include <string.h>

char *getPageFromFile(BM_BufferPool *const bm, const PageNumber pageNum)
{
    SM_FileHandle fileHandle;
    RC result;

    // Open the page file
    result = openPageFile(bm->pageFile, &fileHandle);
    if (result != RC_OK)
    {
        fprintf(stderr, "Error opening page file for reading: %s\n", strerror(result));
        return NULL;
    }

    // Allocate memory for the page data
    char *pageData = (char *)malloc(PAGE_SIZE);
    if (pageData == NULL)
    {
        fprintf(stderr, "Error allocating memory for page data.\n");
        closePageFile(&fileHandle); // Close the file before returning.
        return NULL;
    }

    // Read the page from the file
    result = readBlock(pageNum, &fileHandle, pageData);
    if (result != RC_OK)
    {
        fprintf(stderr, "Error reading page from file: %s\n", strerror(result));
        free(pageData);             // Free the allocated memory before returning.
        closePageFile(&fileHandle); // Close the file before returning.
        return NULL;
    }

    // Close the page file
    result = closePageFile(&fileHandle);
    if (result != RC_OK)
    {
        fprintf(stderr, "Error closing page file after reading: %s\n", strerror(result));
    }

    return pageData;
}

extern int findPage_LRU(const PAGE_FRAME *frames, int numFrames, PageNumber targetPageNum)
{
    int frameIndex = -1;                  // Initialize the frame index to -1 (not found)
    int leastRecentAccessCount = INT_MAX; // Initialize the least recent access count

    int i = 0;
    while (i < numFrames) //run while loop to check if target is empty 
    {
        const PAGE_FRAME currentFrame = frames[i];
        const int isTargetOrEmpty = (currentFrame.pageNum == targetPageNum || currentFrame.pageNum == NO_PAGE);

        if (!isTargetOrEmpty)
        {
            i++;
            continue;
        }

        if (currentFrame.recentAccessCount < leastRecentAccessCount) //check if current frame is recently accessed

            leastRecentAccessCount = currentFrame.recentAccessCount; //set least recent count as current frame count
            frameIndex = i;
        }
        i++;
        
    return frameIndex; //return the result
}

extern int findLRUVictim(const PAGE_FRAME *frames, int numFrames)
{
    int i = 0;

    int leastRecentAccessCount = INT_MAX; // Set the least recent access count to the maximum integer value
    int victimIndex = -1;                 // Set the victim index to -1 if no victim is found

    while (i < numFrames)
    {
        // If the frame is not pinned, and has the least recent access count, update the victim index
        bool isFixCountZero = (frames[i].fixCount == 0);

        if (isFixCountZero)
        {
            bool isRecentAccessLess = frames[i].recentAccessCount < leastRecentAccessCount;
            if (isRecentAccessLess) //check if frame has the least access count

            {
                int newLeastRecentAccessCount = frames[i].recentAccessCount;
                leastRecentAccessCount = newLeastRecentAccessCount; //set least recent access count to new value

                victimIndex = i;
            }
        }
        i++;
    }

    return victimIndex;
}

extern void writePageToFile(BM_BufferPool *const bm, const PAGE_FRAME *frame)
{
    SM_FileHandle fileHandle;
    RC result;

    // Open the page file
    result = openPageFile(bm->pageFile, &fileHandle);
    if (result != RC_OK)
    {
        printf("Error opening page file for writing.\n");
        return;
    }

    // Write the page to the file
    result = writeBlock(frame->pageNum, &fileHandle, frame->data);
    if (result != RC_OK)
    {
        printf("Error writing page to file.\n");
    }

    // Close the page file
    result = closePageFile(&fileHandle);
    if (result != RC_OK)
    {
        printf("Error closing page file after writing.\n");
    }
}

extern bool bufferPoolExists(BM_BufferPool *const bm) //function to check if buffer pool exists
{
    if (bm == NULL || bm->mgmtData == NULL)
    {
        return false;
    }
    return true;
}

extern void updateLRUList(PAGE_FRAME *frames, int numFrames, int accessedFrameIndex)
{
    int i;
    // Update the accessed frame's accessCount to the current highest count
    int highestAccessCount = 0;
    i = 0;
    // Find the highest access count
    while (i < numFrames)
    {
        // If the frame is not pinned, and has the least recent access count, update the victim index
        bool isRecentAccessCountHigher = (frames[i].recentAccessCount > highestAccessCount);

        if (isRecentAccessCountHigher)
        {
            int newAccessCount = frames[i].recentAccessCount;
            highestAccessCount = newAccessCount;
        }
        i++;
    }

    int newAccessCount = highestAccessCount + 1;

    // Update the accessed frame
    frames[accessedFrameIndex].recentAccessCount = newAccessCount;
}

extern int findPage_FIFO(PAGE_FRAME *frames, int numPages, PageNumber pageNum)
{
    int i;
    i = 0;

    while (i < numPages)
    {
        bool isPageMatch = (frames[i].pageNum == pageNum) || (frames[i].pageNum == NO_PAGE);

        if (isPageMatch)
        {
            return i; // Return the index immediately when a match is found
        }
        i++;
    }

    return -1; // Return -1 if no matching frame is found
}

extern int findVictimPage_FIFO(BM_MGMT_DATA *const mgmtData, PAGE_FRAME *frames, int numPages)
{ //function to find victim page

    int head = mgmtData->queueHead;
    int frameIndex = NO_PAGE;
    int newHead;

    for (int count = 0; count < numPages; count++)
    {
        newHead = (head + 1) % numPages;
        head = newHead;
        bool isFixCountZero = (frames[head].fixCount == 0);

        if (isFixCountZero)
        {
            frameIndex = head;
            break;
        }
    }

    return frameIndex;
}

// FIFO Replacement Strategy
extern RC pinPageUsingFIFO(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    // Check if buffer pool is not existing

    if (!bufferPoolExists(bm))
    {
        return RC_BUFFER_POOL_NOT_EXISTING;
    }

    // Get the management data from the buffer pool
    BM_MGMT_DATA *mgmtData;
    PAGE_FRAME *frames;
    int frameIndex;

    mgmtData = (BM_MGMT_DATA *)bm->mgmtData;
    int numPages = bm->numPages;
    frames = mgmtData->frames;

    // Find an available frame using FIFO strategy
    frameIndex = findPage_FIFO(frames, numPages, pageNum);
    RC rc;

    // Now frameIndex contains the index of the page if found, or -1 if not found

    if (frameIndex == -1)
    {
        // Find a victim page using FIFO strategy
        int newIndex = findVictimPage_FIFO(mgmtData, frames, numPages);
        frameIndex = newIndex;

        if (frameIndex == -1)
        {
            getFrameContents(bm);
            rc = RC_NO_AVAILABLE_FRAME;
            return rc;
        }

        // Write the victim page back to disk if dirty
        PAGE_FRAME *currentFrame = &frames[frameIndex];

        if (currentFrame->isDirty)
        {
            BM_PageHandle pageHandle = {.pageNum = currentFrame->pageNum, .data = currentFrame->data};
            forcePage(bm, &pageHandle);
        }
    }

    // Update read count if the page is not already in the buffer pool
    int isDifferentPage = frames[frameIndex].pageNum != pageNum;
    if (isDifferentPage)
    {
        mgmtData->numReadIO++;
    }

    // Update the page handle with the pinned page information
    int queueHead = frameIndex % numPages;
    mgmtData->queueHead = queueHead;

    // Get the page from the file
    frames[frameIndex].data = getPageFromFile(bm, pageNum);

    // Update the page handle with the pinned page information

    int newPageNum = pageNum;
    void *newData = frames[frameIndex].data;

    page->pageNum = newPageNum;
    page->data = newData;

    // Update the frame with the new page information

    PAGE_FRAME *currentFrame = &frames[frameIndex];
    currentFrame->pageNum = pageNum;
    currentFrame->isDirty = false;
    currentFrame->fixCount++;

    return RC_OK;
}

// LRU Replacement Strategy
extern RC pinPageUsingLRU(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    // Check if buffer pool is not existing
    int result = RC_OK; // Define an error code variable

    if (bm == NULL)
    {
        result = RC_BUFFER_POOL_NOT_EXISTING;
        return result;
    }

    // Get the management data from the buffer pool

    PAGE_FRAME *frames;
    BM_MGMT_DATA *mgmtData;
    int frameIndex;

    mgmtData = (BM_MGMT_DATA *)(bm->mgmtData);
    frames = mgmtData->frames;

    // Find the target page in the buffer pool
    int numPages = bm->numPages;
    frameIndex = findPage_LRU(frames, numPages, pageNum);

    // If the page is found in the buffer pool
    if (frameIndex != -1)
    {
        // Update page handle with existing page information

        PAGE_FRAME *frame = &frames[frameIndex];
        frame->data = getPageFromFile(bm, pageNum);

        // Update the page handle with the pinned page information
        page->pageNum = pageNum;
        page->data = frames[frameIndex].data;

        // Update the frame with the new page information

        PAGE_FRAME *currentFrame = &frames[frameIndex];
        currentFrame->pageNum = pageNum;
        currentFrame->isDirty = false;
        currentFrame->fixCount++;

        // Move the page to the front of the LRU list (update accessCount)

        updateLRUList(frames, numPages, frameIndex);

        // Increment read count if the page is not already in the buffer pool
        mgmtData->numReadIO = mgmtData->numReadIO + 1;

        return RC_OK;
    }

    // If the page is not found in the buffer pool
    // Find a victim frame using the LRU strategy
    frameIndex = findLRUVictim(frames, numPages);

    // If a victim frame is found
    if (frameIndex != -1)
    {
        // Write back the victim page if dirty
        PAGE_FRAME *currentFrame = &mgmtData->frames[frameIndex];
        if (currentFrame->isDirty)
        {
            // Write the dirty page back to disk
            writePageToFile(bm, currentFrame);
            mgmtData->numWriteIO++;
        }

        // Update the victim frame with the new page information

        PAGE_FRAME *frame = &mgmtData->frames[frameIndex];
        frame->pageNum = pageNum;
        frame->isDirty = false;
        frame->fixCount = 1;
        frame->data = getPageFromFile(bm, pageNum);

        // mgmtData->numReadIO++;

        // Update page handle with the new page information
        page->pageNum = pageNum;
        page->data = mgmtData->frames[frameIndex].data;

        // Move the page to the front of the LRU list (update accessCount)
        updateLRUList(frames, numPages, frameIndex);

        return RC_OK;
    }

    return RC_NO_AVAILABLE_FRAME;
}

extern RC pinPageUsingLRU_K(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    // Check if buffer pool is not existing
    int result = RC_OK; // Define an error code variable

    if (bm == NULL)
    {
        result = RC_BUFFER_POOL_NOT_EXISTING;
        return result;
    }

    // Get the management data from the buffer pool
    BM_MGMT_DATA *mgmtData;
    PAGE_FRAME *frames;
    int frameIndex;

    mgmtData = (BM_MGMT_DATA *)(bm->mgmtData);
    frames = mgmtData->frames;

    // Find the target page in the buffer pool
    frameIndex = findPage_LRU(frames, bm->numPages, pageNum);

    // If the page is found in the buffer pool
    if (frameIndex != -1)
    {
        // Update page handle with existing page information

        PAGE_FRAME *currentPageFrame = &frames[frameIndex];
        currentPageFrame->data = getPageFromFile(bm, pageNum);

        // Update the page handle with the pinned page information

        int newPageNum = pageNum;
        void *newData = frames[frameIndex].data;

        page->pageNum = newPageNum;
        page->data = newData;

        // Update the frame with the new page information
        PAGE_FRAME *currentFrame = &frames[frameIndex];
        currentFrame->pageNum = pageNum;
        currentFrame->isDirty = false;
        // Increment fix count
        currentFrame->fixCount++;

        // Move the page to the front of the LRU list (update accessCount)
        int np = bm->numPages;
        updateLRUList(frames, np, frameIndex);

        mgmtData->numReadIO++;

        return RC_OK;
    }

    // If the page is not found in the buffer pool
    // Find a victim frame using the LRU strategy
    int np = bm->numPages;
    frameIndex = findLRUVictim(frames, np);

    // If a victim frame is found
    if (frameIndex != -1)
    {
        // Write back the victim page if dirty
        PAGE_FRAME *currentFrame = &mgmtData->frames[frameIndex];

        if (currentFrame->isDirty)
        {
            writePageToFile(bm, currentFrame);
            mgmtData->numWriteIO++;
        }

        // Update the victim frame with the new page information

        currentFrame->pageNum = pageNum;
        currentFrame->isDirty = false;
        currentFrame->fixCount = 1;
        currentFrame->data = getPageFromFile(bm, pageNum);

        // Update page handle with the new page information
        page->pageNum = pageNum;
        page->data = mgmtData->frames[frameIndex].data;

        // Move the page to the front of the LRU list (update accessCount)
        updateLRUList(frames, np, frameIndex);

        return RC_OK;
    }

    return RC_NO_AVAILABLE_FRAME;
}

// "bufferSize" represents the size of the buffer pool i.e. maximum number of page frames that can be kept into the buffer pool
int bufferSize = 0;

// "rearIndex" basically stores the count of number of pages read from the disk.
// "rearIndex" is also used by FIFO function to calculate the frontIndex i.e.
int rearIndex = 0;

// "writeCount" counts the number of I/O write to the disk i.e. number of pages writen to the disk
int writeCount = 0;

// "hit" a general count which is incremented whenever a page frame is added into the buffer pool.
// "hit" is used by LRU to determine least recently added page into the buffer pool.
int hit = 0;

// "clockPointer" is used by CLOCK algorithm to point to the last added page in the buffer pool.
int clockPointer = 0;

// "lfuPointer" is used by LFU algorithm to store the least frequently used page frame's position. It speeds up operation  from 2nd replacement onwards.
int lfuPointer = 0;

// Defining FIFO (First In First Out) function
extern void FIFO(BM_BufferPool *const bm, PageFrame *page)
{
	//printf("FIFO Started");
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	
	int i, frontIndex;
	frontIndex = rearIndex % bufferSize;

	// Interating through all the page frames in the buffer pool
	for(i = 0; i < bufferSize; i++)
	{
		if(pageFrame[frontIndex].fixCount == 0)
		{
			// If page in memory has been modified (dirtyBit = 1), then write page to disk
			if(pageFrame[frontIndex].dirtyBit == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(pageFrame[frontIndex].pageNum, &fh, pageFrame[frontIndex].data);
				
				// Increase the writeCount which records the number of writes done by the buffer manager.
				writeCount++;
			}
			
			// Setting page frame's content to new page's content
			pageFrame[frontIndex].data = page->data;
			pageFrame[frontIndex].pageNum = page->pageNum;
			pageFrame[frontIndex].dirtyBit = page->dirtyBit;
			pageFrame[frontIndex].fixCount = page->fixCount;
			break;
		}
		else
		{
			// If the current page frame is being used by some client, we move on to the next location
			frontIndex++;
			frontIndex = (frontIndex % bufferSize == 0) ? 0 : frontIndex;
		}
	}
}

// Defining LFU (Least Frequently Used) function
extern void LFU(BM_BufferPool *const bm, PageFrame *page)
{
	//printf("LFU Started");
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	
	int i, j, leastFreqIndex, leastFreqRef;
	leastFreqIndex = lfuPointer;	
	
	// Interating through all the page frames in the buffer pool
	for(i = 0; i < bufferSize; i++)
	{
		if(pageFrame[leastFreqIndex].fixCount == 0)
		{
			leastFreqIndex = (leastFreqIndex + i) % bufferSize;
			leastFreqRef = pageFrame[leastFreqIndex].refNum;
			break;
		}
	}

	i = (leastFreqIndex + 1) % bufferSize;

	// Finding the page frame having minimum refNum (i.e. it is used the least frequent) page frame
	for(j = 0; j < bufferSize; j++)
	{
		if(pageFrame[i].refNum < leastFreqRef)
		{
			leastFreqIndex = i;
			leastFreqRef = pageFrame[i].refNum;
		}
		i = (i + 1) % bufferSize;
	}
		
	// If page in memory has been modified (dirtyBit = 1), then write page to disk	
	if(pageFrame[leastFreqIndex].dirtyBit == 1)
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(pageFrame[leastFreqIndex].pageNum, &fh, pageFrame[leastFreqIndex].data);
		
		// Increase the writeCount which records the number of writes done by the buffer manager.
		writeCount++;
	}
	
	// Setting page frame's content to new page's content		
	pageFrame[leastFreqIndex].data = page->data;
	pageFrame[leastFreqIndex].pageNum = page->pageNum;
	pageFrame[leastFreqIndex].dirtyBit = page->dirtyBit;
	pageFrame[leastFreqIndex].fixCount = page->fixCount;
	lfuPointer = leastFreqIndex + 1;
}

// Defining LRU (Least Recently Used) function
extern void LRU(BM_BufferPool *const bm, PageFrame *page)
{	
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	int i, leastHitIndex, leastHitNum;

	// Interating through all the page frames in the buffer pool.
	for(i = 0; i < bufferSize; i++)
	{
		// Finding page frame whose fixCount = 0 i.e. no client is using that page frame.
		if(pageFrame[i].fixCount == 0)
		{
			leastHitIndex = i;
			leastHitNum = pageFrame[i].hitNum;
			break;
		}
	}	

	// Finding the page frame having minimum hitNum (i.e. it is the least recently used) page frame
	for(i = leastHitIndex + 1; i < bufferSize; i++)
	{
		if(pageFrame[i].hitNum < leastHitNum)
		{
			leastHitIndex = i;
			leastHitNum = pageFrame[i].hitNum;
		}
	}

	// If page in memory has been modified (dirtyBit = 1), then write page to disk
	if(pageFrame[leastHitIndex].dirtyBit == 1)
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(pageFrame[leastHitIndex].pageNum, &fh, pageFrame[leastHitIndex].data);
		
		// Increase the writeCount which records the number of writes done by the buffer manager.
		writeCount++;
	}
	
	// Setting page frame's content to new page's content
	pageFrame[leastHitIndex].data = page->data;
	pageFrame[leastHitIndex].pageNum = page->pageNum;
	pageFrame[leastHitIndex].dirtyBit = page->dirtyBit;
	pageFrame[leastHitIndex].fixCount = page->fixCount;
	pageFrame[leastHitIndex].hitNum = page->hitNum;
}

// Defining CLOCK function
extern void CLOCK(BM_BufferPool *const bm, PageFrame *page)
{	
	//printf("CLOCK Started");
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	while(1)
	{
		clockPointer = (clockPointer % bufferSize == 0) ? 0 : clockPointer;

		if(pageFrame[clockPointer].hitNum == 0)
		{
			// If page in memory has been modified (dirtyBit = 1), then write page to disk
			if(pageFrame[clockPointer].dirtyBit == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(pageFrame[clockPointer].pageNum, &fh, pageFrame[clockPointer].data);
				
				// Increase the writeCount which records the number of writes done by the buffer manager.
				writeCount++;
			}
			
			// Setting page frame's content to new page's content
			pageFrame[clockPointer].data = page->data;
			pageFrame[clockPointer].pageNum = page->pageNum;
			pageFrame[clockPointer].dirtyBit = page->dirtyBit;
			pageFrame[clockPointer].fixCount = page->fixCount;
			pageFrame[clockPointer].hitNum = page->hitNum;
			clockPointer++;
			break;	
		}
		else
		{
			// Incrementing clockPointer so that we can check the next page frame location.
			// We set hitNum = 0 so that this loop doesn't go into an infinite loop.
			pageFrame[clockPointer++].hitNum = 0;		
		}
	}
}
