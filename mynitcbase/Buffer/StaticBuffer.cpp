#include "StaticBuffer.h"
#include<string.h>
#include<stdio.h>
unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];
StaticBuffer::StaticBuffer() {
    // copy blockAllocMap blocks from disk to buffer (using readblock() of disk)
  // blocks 0 to 3
  unsigned char buffer[BLOCK_SIZE];
     Disk::readBlock(blockAllocMap,0);
     Disk::readBlock(blockAllocMap+1*BLOCK_SIZE,1);
     Disk::readBlock(blockAllocMap+2*BLOCK_SIZE,2);
     Disk::readBlock(blockAllocMap+3*BLOCK_SIZE,3);


     // initialise all blocks as free
    for(int bufferIndex = 0; bufferIndex <BUFFER_CAPACITY ; bufferIndex++) {
         
        metainfo[bufferIndex].free = true;
        metainfo[bufferIndex].dirty = false;
        metainfo[bufferIndex].timeStamp= -1;
        metainfo[bufferIndex].blockNum= -1;
    }
}
/*
At this stage, we are not writing back from the buffer to the disk since we are
not modifying the buffer. So, we will define an empty destructor for now. In
subsequent stages, we will implement the write-back functionality here.
*/
StaticBuffer::~StaticBuffer() {
       
     Disk::writeBlock(blockAllocMap,0);
     Disk::writeBlock(blockAllocMap+1*BLOCK_SIZE,1);
     Disk::writeBlock(blockAllocMap+2*BLOCK_SIZE,2);
     Disk::writeBlock(blockAllocMap+3*BLOCK_SIZE,3);



      for(int bufferIndex = 0; bufferIndex <BUFFER_CAPACITY ; bufferIndex++) {
         
        if(metainfo[bufferIndex].free == false && metainfo[bufferIndex].dirty == true) {
            Disk::writeBlock(blocks[bufferIndex],metainfo[bufferIndex].blockNum);
        }
      }
 }

int StaticBuffer::getFreeBuffer(int blockNum) {

    // Check if blockNum is valid (non zero and less than DISK_BLOCKS)
    // and return E_OUTOFBOUND if not valid.
    if(blockNum < 0 || blockNum >= DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }
    for (int bufferIndex=0;bufferIndex<BUFFER_CAPACITY;bufferIndex++) {
      if(metainfo[bufferIndex].free==false)
      metainfo[bufferIndex].timeStamp++;
    }
     // let bufferNum be used to store the buffer number of the free
    int bufferNum = -1;
    
  for(int bufferIndex = 0; bufferIndex <BUFFER_CAPACITY ; bufferIndex++) {

      if(metainfo[bufferIndex].free == true) {
          bufferNum = bufferIndex;
          break;
      }
    }

    if(bufferNum == -1) {
        int time = -1;

        for(int bufferIndex = 0; bufferIndex <BUFFER_CAPACITY ; bufferIndex++) {

      if(metainfo[bufferIndex].timeStamp > time) {
          bufferNum = bufferIndex;
          time = metainfo[bufferIndex].timeStamp;
         }
        }

        if(metainfo[bufferNum].dirty == true) {

            Disk::writeBlock(blocks[bufferNum],metainfo[bufferNum].blockNum);
        }
    }
  
   metainfo[bufferNum].free = false;
   metainfo[bufferNum].blockNum = blockNum;
   metainfo[bufferNum].dirty = false;
   metainfo[bufferNum].timeStamp = 0;

   return bufferNum;

}
/* Get the buffer index where a particular block is stored
   or E_BLOCKNOTINBUFFER otherwise
*/
int StaticBuffer::getBufferNum(int blockNum) {
        // Check if blockNum is valid (between zero and DISK_BLOCKS)
         if( blockNum <0 || blockNum >= DISK_BLOCKS) {
            return E_OUTOFBOUND; // and return E_OUTOFBOUND if not valid.
         }
           
          // find and return the bufferIndex which corresponds to blockNum (check metainfo)
    for(int index = 0; index <BUFFER_CAPACITY; index++) {

        if( metainfo[index].free == false && metainfo[index].blockNum == blockNum) {

             return index;
        }
    }

      // if block is not in the buffer 
    return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum){
    // find the buffer index corresponding to the block using getBufferNum().
     int bufferNum = StaticBuffer::getBufferNum(blockNum);
    // if block is not present in the buffer (bufferNum = E_BLOCKNOTINBUFFER)
    //     return E_BLOCKNOTINBUFFER
    if(bufferNum == E_BLOCKNOTINBUFFER)
    return E_BLOCKNOTINBUFFER;

    // if blockNum is out of bound (bufferNum = E_OUTOFBOUND)
    //     return E_OUTOFBOUND
      if(bufferNum == E_OUTOFBOUND)
      return E_OUTOFBOUND;

         
      metainfo[bufferNum].dirty = true;

    // else
    //     (the bufferNum is valid)
    //     set the dirty bit of that buffer to true in metainfo

    return SUCCESS;
}

int StaticBuffer::getStaticBlockType(int blockNum){
    // Check if blockNum is valid (non zero and less than number of disk blocks)
    // and return E_OUTOFBOUND if not valid.
    if(blockNum <0 || blockNum>=DISK_BLOCKS)
    return E_OUTOFBOUND;

    // Access the entry in block allocation map corresponding to the blockNum argument
    // and return the block type after type casting to integer.
    int blockType = (int )StaticBuffer::blockAllocMap[blockNum];
    return blockType;
}
