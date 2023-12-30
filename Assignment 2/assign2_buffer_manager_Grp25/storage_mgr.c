#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
#include "helper.c"

SM_FileHandle *fileHandle;

/* manipulating page files */

RC createPageFile(char *fileName)
{
    // create file pointer and open to write
    FILE *file = fopen(fileName, "wb"); // write in binary mode
    // check if file already exists
    if (file == NULL)
    {
        return RC_FILE_NOT_FOUND; // return error message if file is not found
    }
    // using stack allocated buffer to create empty page buffer

    char emptyPage[PAGE_SIZE] = {'\0'}; // creates empty page and initializes the bytes to \0

    // Write the empty page to the file using fwrite
    fwrite(emptyPage, sizeof(char), PAGE_SIZE, file); // successfully writes
    // close the file after the write operation
    fclose(file);
    // return OK message if Successfully created file
    return RC_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    FILE *file = fopen(fileName, "rb+"); // open file in binary mode to perform operations
    // check if file is existing and return error if not found
    if (file == NULL)
    {
        return RC_FILE_NOT_FOUND; // File not found
    }

    // Initialize the file handle fields
    fHandle->fileName = strdup(fileName); // copy the file name string and store in handle to keep track of name associated with file
    fHandle->totalNumPages = -1;          // Initialize totalNumPages to -1
    fHandle->curPagePos = 0;              // set current page position to 0, beginning of file; used to keep track of current page being accessed
    fHandle->mgmtInfo = file;             // store the file as mgmtinfo; to maintain a reference to open file

    // Seek to the end of the file to determine its size
    fseek(file, 0, SEEK_END);
    // ftell is called to determine current position of file indicator
    long fileSize = ftell(file); // store the filesize

    // Calculate totalNumPages based on file size
    fHandle->totalNumPages = (int)((fileSize + PAGE_SIZE - 1) / PAGE_SIZE);

    // Rewind to the beginning of the file
    rewind(file);

    // return RC_OK if successful
    return RC_OK; // Page file opened successfully
}

// Close an open page file
RC closePageFile(SM_FileHandle *fHandle)
{
    if (fHandle->mgmtInfo != NULL) // check if file is open
    {
        fclose((FILE *)(fHandle->mgmtInfo)); // Close the file
    }
    // Free memory allocated for fileName
    if (fHandle->fileName != NULL)
    {
        free(fHandle->fileName);
    }
    return RC_OK; // Page file closed successfully
}

RC destroyPageFile(char *fileName)
{
    if (remove(fileName) == 0)
    {
        return RC_OK; // Page file destroyed successfully
    }
    if (remove(fileName) != 0)
    {
        return RC_FILE_NOT_FOUND; // File not found or unable to destroy
    }
}

void initStorageManager(void)
{
    // Initialize the fileHandle attributes to default values
    resetHandle(fileHandle);
}

/* reading blocks from disc */
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    FILE *file = NULL;
    file = fopen(fHandle->fileName, "r+");

    if (file == NULL) // check if file exists or not
    {
        // Return a read error code if file open failed
        return RC_FILE_NOT_FOUND;
    }

    // We can now move the file pointer to a specific position in the file
    // The desired block is calculated by multiplying pageNum with PAGE_SIZE
    long desired_block = pageNum * PAGE_SIZE;

    if (fseek(file, desired_block, SEEK_SET) != 0)
    {
        // Return a read error code if fseek failed
        fclose(file); // Close the file before returning the error
        return RC_READ_NON_EXISTING_PAGE;
    }

    //  Read the content of the page into the memory page buffer
    size_t readPage = fread(memPage, sizeof(char), PAGE_SIZE, file);

    // We update the current page position in the file handle after reading the content.
    fHandle->curPagePos = ftell(file);

    // Close the file
    fclose(file);

    // Return a success code
    return RC_OK;
}
// Read a specific block from the file

// Get the current block position of the file handle
int getBlockPos(SM_FileHandle *fHandle)
{
    // Return the current page position stored in the file handle
    int currentBlockPosition = fHandle->curPagePos;
    return currentBlockPosition;
}

// Read the first block of the file
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Call the readBlock function with pageNum 0 which is a first block
    int firstBlock;
    firstBlock = 0;
    return readBlock(firstBlock, fHandle, memPage);
}

// Read the previous block relative to the current position
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Calculate the page number of the previous block
    int prevPageNum;
    prevPageNum = fHandle->curPagePos - 1;

    // Now, with the calculated previous page number call the readBlock function
    return readBlock(prevPageNum, fHandle, memPage);
}

// Read the current block
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int currPage;
    currPage = fHandle->curPagePos;
    // Call the readBlock function with the current page position
    return readBlock(currPage, fHandle, memPage);
}

// Read the next block relative to the current position
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // We will calculate the page number of the next block and pass it to read block.
    int nextPage;
    nextPage = fHandle->curPagePos + 1;

    // Now, with the calculated next page number, we will call the readBlock function
    return readBlock(nextPage, fHandle, memPage);
}

// Read the last block of the file
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // To read last block, we will calculate the page number of the last block
    int lastPageNum = fHandle->totalNumPages - 1;

    // Call the readBlock function with the calculated last page number
    return readBlock(lastPageNum, fHandle, memPage);
}

// Write page to a disk using absolute position
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // We will initialize the file pointer for file operations.
    FILE *file = NULL;
    long desired_block;
    RC rc;
    desired_block = pageNum * PAGE_SIZE;   // Here, we are calculating desired block
    file = fopen(fHandle->fileName, "r+"); // Open file to read and write

    if (file == NULL) // Check if file can be accessed
        return RC_FILE_NOT_FOUND;

    if (fHandle == NULL) // Checking if file handle is initialized
        return RC_FILE_HANDLE_NOT_INIT;

    if (pageNum > fHandle->totalNumPages || pageNum < 0)
        rc = RC_WRITE_FAILED;

    /*else if (pageNum > fHandle->totalNumPages || pageNum < 0) // Check page number boundries
        return RC_WRITE_FAILED;*/

    // Now, we will write the page to the file if all above conditions are false .
    // We will move file pointer to a specific page in the file by calculating the offset as pageNum * PAGE_SIZE
    if (fseek(file, desired_block, SEEK_SET) == 0)
    {
        // fseek operation is successful which means file position was moved to desired page.
        fwrite(memPage, 1, PAGE_SIZE, file); // Now, write content to the file
        fHandle->curPagePos = pageNum;       // Update current position
        // return RC_OK if successful
        return RC_OK;
    }

    // If fseek fails, set the return code to indicate a write failure.
    else if (fseek(file, desired_block, SEEK_SET) != 0)
        return RC_WRITE_FAILED;

    fclose(file);
    return rc;
}

// Using current position, write a page to disk
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (fHandle == NULL) // check if file handle is initialized
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // If fHandle initialized, retrieve current block position and using writeblock, perform write operation
    int cp = getBlockPos(fHandle); // position of current page
    // return write block with current page position
    return writeBlock(cp, fHandle, memPage);
}

// Increase the number of pages in the file by 1.
RC appendEmptyBlock(SM_FileHandle *fHandle)
{

    if (fHandle == NULL) // check if file handle is initialized
        return RC_FILE_HANDLE_NOT_INIT;

    FILE *file = NULL;
    // Open the file in read and write mode
    file = fopen(fHandle->fileName, "r+");

    if (file == NULL) // check if file can be opened
        return RC_FILE_NOT_FOUND;

    // Now to append data, we will seek to the end of the file
    fseek(file, 0, SEEK_END);

    // Increase the total number of pages in the file handle
    fHandle->totalNumPages += 1;

    // Allocate memory for an empty page using calloc
    // Allocate block of memory of size = page_size

    char *emptyPage = (char *)calloc(PAGE_SIZE, sizeof(char));
    if (emptyPage == NULL)
    {
        // Check if memory allocation fails
        fclose(file);
        return RC_READ_NON_EXISTING_PAGE;
    }

    // write the empty page to the end of file
    fwrite(emptyPage, sizeof(char), PAGE_SIZE, file);

    // free the allocated memory for the empty page
    free(emptyPage);

    // close the file
    fclose(file);

    return RC_OK; // return RC_OK if successful
}

//  Increase the size to numberOfPages if file has less than numberOfPages pages
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
    int totalPages;
    if (fHandle == NULL) // check if file handle is initialized
        return RC_FILE_HANDLE_NOT_INIT;

    int remPages;
    totalPages = fHandle->totalNumPages;
    remPages = numberOfPages - totalPages; // Here, we are calculating remaining pages

    if (fHandle != NULL)
    {
        if (numberOfPages > totalPages) // Check if the desired number of pages is greater than the file's current page count.
        {
            // Now, append empty new pages until it reaches the desired the capacity.
            while (remPages > 0)
                appendEmptyBlock(fHandle); // Attach empty block to the file.
        }
        // return RC_OK if successful
        return RC_OK;
    }
}
