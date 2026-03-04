#include "BlockBuffer.h"
#include<iostream>
#include <cstdlib>
#include <cstring>

BlockBuffer::BlockBuffer(int blockNum){
    this->blockNum=blockNum;
}
int BlockBuffer::getBlockNum(){
  return this->blockNum;
}
BlockBuffer::BlockBuffer(char blockType){
    // allocate a block on the disk and a buffer in memory to hold the new block of
    // given type using getFreeBlock function and get the return error codes if any.
    int type;
    if(blockType == 'R')
    type = REC;
    else if(blockType == 'I')
    type = IND_INTERNAL;
    else if(blockType == 'L')
    type = IND_LEAF;
    int blockNum = BlockBuffer::getFreeBlock(type);

    // set the blockNum field of the object to that of the allocated block
    // number if the method returned a valid block number,
    // otherwise set the error code returned as the block number.
    this->blockNum = blockNum;

    // (The caller must check if the constructor allocatted block successfully
    // by checking the value of block number field.)
}

RecBuffer::RecBuffer(int blockNum) : BlockBuffer :: BlockBuffer(blockNum){}
RecBuffer::RecBuffer():BlockBuffer('R'){}
/*
Used to get the header of the block into the location pointed to by `head`
NOTE: this function expects the caller to allocate memory for `head`
*/

int BlockBuffer::getHeader(struct HeadInfo *head) {

     unsigned char *bufferPtr;
      
     int ret = loadBlockAndGetBufferPtr(&bufferPtr);

     if( ret != SUCCESS) {
        return ret; // return any errors that might have occured in the process
     }
      // ... (the rest of the logic is as in stage 2)
     memcpy(&head->blockType,bufferPtr,4);
      
     memcpy(&head->pblock,bufferPtr+4,4);
        
     memcpy(&head->lblock,bufferPtr+8,4);
     
     memcpy(&head->rblock,bufferPtr+12,4);

     memcpy(&head->numEntries,bufferPtr+16,4);

     memcpy(&head->numAttrs,bufferPtr+20,4);
      
     memcpy(&head->numSlots,bufferPtr+24,4);
      
      
      
      return SUCCESS;
}
/*
Used to get the record at slot `slotNum` into the array `rec`
NOTE: this function expects the caller to allocate memory for `rec`
*/
int RecBuffer::getRecord(union Attribute *rec,int slotNum) {

    struct HeadInfo head;
    
    this->getHeader(&head);
    
    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;
    
    unsigned char *bufferPtr;
      
      int ret = loadBlockAndGetBufferPtr(&bufferPtr);
      if( ret != SUCCESS) {
         return ret;
      }
      // ... (the rest of the logic is as in stage 2

      int recordSize=attrCount*ATTR_SIZE;
      
      int offset=HEADER_SIZE+slotCount+recordSize*slotNum;
      
      unsigned char *slotPointer=bufferPtr+offset;
      
      memcpy(rec,slotPointer,recordSize);
      
      return SUCCESS;
      
}
/*
Used to load a block to the buffer and get a pointer to it.
NOTE: this function expects the caller to allocate memory for the argument
*/
int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffer) {
  // check whether the block is already present in the buffer using StaticBuffer.getBufferNum()
    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

    if(bufferNum != E_BLOCKNOTINBUFFER) 
    {
         for(int bufferIndex = 0; bufferIndex <BUFFER_CAPACITY ; bufferIndex++) {

            if(bufferIndex != bufferNum) {
                StaticBuffer::metainfo[bufferIndex].timeStamp += 1;
              }
              else {
                StaticBuffer::metainfo[bufferIndex].timeStamp = 0;
              }
        }
    }
    else {

        bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

        if(bufferNum == E_OUTOFBOUND) {

            return E_OUTOFBOUND;
        }

        Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);

    }
     // store the pointer to this buffer (blocks[bufferNum]) in *buffPtr
    *buffer = StaticBuffer::blocks[bufferNum];

    return SUCCESS;

}

//stage4

/* used to get the slotmap from a record block
NOTE: this function expects the caller to allocate memory for `*slotMap`
*/
int RecBuffer::getSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;

  // get the starting address of the buffer containing the block using loadBlockAndGetBufferPtr().
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  struct HeadInfo head;
  // get the header of the block using getHeader() function
  this->getHeader(&head);

  int slotCount = head.numSlots/* number of slots in block from header */;

  // get a pointer to the beginning of the slotmap in memory by offsetting HEADER_SIZE
  unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;

  // copy the values from `slotMapInBuffer` to `slotMap` (size is `slotCount`)
  memcpy(slotMap,slotMapInBuffer,slotCount);

  return SUCCESS;
}

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType) {
    
    double diff;

     if ( attrType == STRING)
         diff = strcmp(attr1.sVal, attr2.sVal);
     else 
         diff = attr1.nVal - attr2.nVal;

     if( diff > 0)
       return 1;
     if( diff < 0)
       return -1;
       return 0;
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum) {
    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
       int ret = RecBuffer::loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
        if(ret != SUCCESS)
        return ret;

    /* get the header of the block using the getHeader() function */
    HeadInfo header;
    RecBuffer::getHeader(&header);

    // get number of attributes in the block.
    int numattrs = header.numAttrs;

    // get the number of slots in the block.
    int numslots = header.numSlots;

    // if input slotNum is not in the permitted range return E_OUTOFBOUND.

    if(slotNum < 0 || slotNum >= numslots) {
        return E_OUTOFBOUND;
    }

    /* offset bufferPtr to point to the beginning of the record at required
       slot. the block contains the header, the slotmap, followed by all
       the records. so, for example,
       record at slot x will be at bufferPtr + HEADER_SIZE + (x*recordSize)
       copy the record from `rec` to buffer using memcpy
       (hint: a record will be of size ATTR_SIZE * numAttrs)
    */
     int recSize = ATTR_SIZE*numattrs;
     unsigned char *recPtr = bufferPtr + HEADER_SIZE + numslots + slotNum*recSize;

     memcpy(recPtr,rec,recSize);
    // update dirty bit using setDirtyBit()
      StaticBuffer::setDirtyBit(this->blockNum);

    /* (the above function call should not fail since the block is already
       in buffer and the blockNum is valid. If the call does fail, there
       exists some other issue in the code) */

    return SUCCESS;
}

int BlockBuffer::setHeader(struct HeadInfo *head){

    unsigned char *bufferPtr;
    // get the starting address of the buffer containing the block using
    // loadBlockAndGetBufferPtr(&bufferPtr).
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
        if(ret!=SUCCESS)
        return ret;

    // cast bufferPtr to type HeadInfo*
    struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;

    // copy the fields of the HeadInfo pointed to by head (except reserved) to
    // the header of the block (pointed to by bufferHeader)
    //(hint: bufferHeader->numSlots = head->numSlots )
    memcpy(bufferHeader,head,28);

    // update dirty bit by calling StaticBuffer::setDirtyBit()
    // if setDirtyBit() failed, return the error code
    int retVal = StaticBuffer::setDirtyBit(this->blockNum);
    if(retVal!=SUCCESS)
    return retVal;

    return SUCCESS;
}

int BlockBuffer::setBlockType(int blockType){

    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
       int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
         if(ret!=SUCCESS)
        return ret;

    // store the input block type in the first 4 bytes of the buffer.
    // (hint: cast bufferPtr to int32_t* and then assign it)
    // *((int32_t *)bufferPtr) = blockType;
     *((int32_t *)bufferPtr) = blockType;

    // update the StaticBuffer::blockAllocMap entry corresponding to the
    // object's block number to `blockType`.
    StaticBuffer::blockAllocMap[this->blockNum] = blockType;

    // update dirty bit by calling StaticBuffer::setDirtyBit()
    // if setDirtyBit() failed
        // return the returned value from the call
     ret = StaticBuffer::setDirtyBit(this->blockNum);
    if(ret!=SUCCESS)
    return ret;

    return SUCCESS;
}

int BlockBuffer::getFreeBlock(int blockType){

    // iterate through the StaticBuffer::blockAllocMap and find the block number
    // of a free block in the disk.
    int blockNum = -1;
    for(int i = 7;i<DISK_BLOCKS;i++){
      if(StaticBuffer::blockAllocMap[i] == UNUSED_BLK)
       {
          blockNum = i;
          break;
       }
    }
    

    // if no block is free, return E_DISKFULL.
    if(blockNum == -1)
    return E_DISKFULL;

    // set the object's blockNum to the block number of the free block.
    this->blockNum = blockNum;

    // find a free buffer using StaticBuffer::getFreeBuffer() .
    StaticBuffer::getFreeBuffer(this->blockNum);

    // initialize the header of the block passing a struct HeadInfo with values
    // pblock: -1, lblock: -1, rblock: -1, numEntries: 0, numAttrs: 0, numSlots: 0

    HeadInfo head;
    head.pblock = -1;
    head.lblock = -1;
    head.rblock = -1;
    head.numEntries = 0;
    head.numAttrs = 0;
    head.numSlots = 0;
    // to the setHeader() function.
    BlockBuffer::setHeader(&head);
    // update the block type of the block to the input block type using setBlockType().
    BlockBuffer::setBlockType(blockType);

    // return block number of the free block.
    return blockNum;
}

int RecBuffer::setSlotMap(unsigned char *slotMap) {
    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block using
       loadBlockAndGetBufferPtr(&bufferPtr). */
      int ret = RecBuffer::loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
        if(ret != SUCCESS)
        return ret;

    // get the header of the block using the getHeader() function
    HeadInfo head;
    RecBuffer::getHeader(&head);
     /* the number of slots in the block */
    int numSlots = head.numSlots;

    // the slotmap starts at bufferPtr + HEADER_SIZE. Copy the contents of the
    // argument `slotMap` to the buffer replacing the existing slotmap.
    // Note that size of slotmap is `numSlots`
    memcpy(bufferPtr+HEADER_SIZE,slotMap,numSlots);

    // update dirty bit using StaticBuffer::setDirtyBit
    // if setDirtyBit failed, return the value returned by the call
     ret = StaticBuffer::setDirtyBit(this->blockNum);
    if(ret != SUCCESS)
    return ret;

    return SUCCESS;
}
