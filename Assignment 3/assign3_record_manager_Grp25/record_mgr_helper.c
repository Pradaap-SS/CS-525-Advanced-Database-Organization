#define RECORD_MGR_H

#include "dberror.h"
#include "expr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

int findFreeSlot(char *data, int recordSize)
{
    int i = 0;
    int totalSlots = PAGE_SIZE / recordSize;

    while (i < totalSlots)
    {
        if (data[i * recordSize] != '+')
        {
            return i;
        }
        i++;
    }

    return -1;
}