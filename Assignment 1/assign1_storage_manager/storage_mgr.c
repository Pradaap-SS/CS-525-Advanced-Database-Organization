/*
Functions for reading data blocks from a file into memory pages 
*/

// Reads a specific block/page from file into a page handle
RC readBlock(int pageNum, FileHandle* fHandle, PageHandle* memPage) {

  FILE* fp = fopen(fHandle->fileName, "r+");
  
  if(fseek(fp, pageNum * PAGE_SIZE, SEEK_SET) != 0) {
    // Failed to seek, return error
    fclose(fp); 
    return ERROR_READING_PAGE; 
  }

  // Read page data into memory
  fread(memPage, sizeof(char), PAGE_SIZE, fp);

  // Save current position
  fHandle->curPagePos = ftell(fp);

  fclose(fp);
  return OK;
}

// Get current page position
int getCurPagePos(FileHandle* fHandle) {
  return fHandle->curPagePos;
}

// Read first page
RC readFirstPage(FileHandle* fHandle, PageHandle* memPage) {
  return readBlock(0, fHandle, memPage); 
}

// Read previous page 
RC readPrevPage(FileHandle* fHandle, PageHandle* memPage) {
  return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}

// Read current page
RC readCurPage(FileHandle* fHandle, PageHandle* memPage) {
  return readBlock(fHandle->curPagePos, fHandle, memPage);
}

// Read next page
RC readNextPage(FileHandle* fHandle, PageHandle* memPage) {
  return readBlock(fHandle->curPagePos + 1, fHandle, memPage);  
}

// Read last page
RC readLastPage(FileHandle* fHandle, PageHandle* memPage) {
  return readBlock(fHandle->totalPages - 1, fHandle, memPage);
}