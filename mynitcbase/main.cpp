#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include<iostream>
#include<cstring>
using namespace std;
void stage2();
void stage3();
void stage3_exe();
int main(int argc, char *argv[]) {


    //stage2();
    //stage3();
    //stage3_exe();

    //stage5;
     Disk disk_run;
    StaticBuffer buffer;
    OpenRelTable cache;

    return FrontendInterface::handleFrontend(argc, argv);
  
}
void stage3_exe() {

    Disk disk_run;
    StaticBuffer buffer;
    OpenRelTable cache;

    RecBuffer relCatBuffer(RELCAT_BLOCK);
    Attribute relCatRecord[RELCAT_NO_ATTRS];

    RelCatEntry relCatEntry;

    RelCacheTable::getRelCatEntry(RELCAT_RELID,&relCatEntry);
     bool find = false;
    for(int i = 0; i < relCatEntry.numRecs; i++) {

        relCatBuffer.getRecord(relCatRecord,i);
        
        if(strcmp(relCatRecord[RELCAT_REL_NAME_INDEX].sVal,"Students")==0) {

            printf("Relation: %s\n",relCatRecord[RELCAT_REL_NAME_INDEX].sVal);
            find = true;
            break;
        }
    }
    if(find == false) {
        
      printf("Student Relation is not found\n");
      return ;
    }
     int blockNum = ATTRCAT_BLOCK;
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    HeadInfo attrCatHeader;

    RelCatEntry attrCatEntry;
    RelCacheTable::getRelCatEntry(ATTRCAT_RELID,&attrCatEntry);
    for(int i =0;i < attrCatEntry.numRecs;) {

      RecBuffer attrCatBuffer(blockNum);
      attrCatBuffer.getHeader(&attrCatHeader);

      for( int j = 0; j < attrCatEntry.numSlotsPerBlk && i < attrCatEntry.numRecs; j++,i++) {

           attrCatBuffer.getRecord(attrCatRecord,j);
           
           if(strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,"Students") == 0)
           {
              const char *attrType=attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal==NUMBER?"NUM":"STR";
            printf(" %s:%s\n",attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,attrType);
           }
      }
      blockNum = attrCatHeader.rblock;
      if(blockNum == -1)
      break;

    }
    
}
void stage3() {
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;

  RelCatEntry relCatBuf;
  AttrCatEntry attrCatBuf;
  
  for(int i = 0; i <=1 ; i++) {
      RelCacheTable::getRelCatEntry(i, &relCatBuf);
      printf("Relation: %s\n", relCatBuf.relName);

      for(int j = 0; j < relCatBuf.numAttrs; j++) {

         AttrCacheTable::getAttrCatEntry(i,j,&attrCatBuf);
         const char *attrType=attrCatBuf.attrType==NUMBER?"NUM":"STR";
         printf(" %s:%s\n",attrCatBuf.attrName,attrType);   
       }

       printf("\n");

  }

}

void stage2()
{
     /* Initialize the Run Copy of Disk */
  Disk disk_run;
  RecBuffer relCatBuffer(RELCAT_BLOCK);
  HeadInfo relCatHeader;
  HeadInfo attrCatHeader;
  
  relCatBuffer.getHeader(&relCatHeader);
  
  for(int i=0;i<relCatHeader.numEntries;i++)
  {
      Attribute relCatRecord[relCatHeader.numAttrs];
      relCatBuffer.getRecord(relCatRecord,i);
      printf("Relation: %s\n",relCatRecord[0].sVal);
      int header=5;
      while(1){
        RecBuffer attrCatBuffer(header);
       attrCatBuffer.getHeader(&attrCatHeader);
      for(int j=0;j<attrCatHeader.numEntries;j++)
      {
         Attribute attrCatRecord[attrCatHeader.numAttrs];
         attrCatBuffer.getRecord(attrCatRecord,j);
         if(strcmp(attrCatRecord[0].sVal,relCatRecord[0].sVal)==0)
         {
            const char *attrType=attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal==NUMBER?"NUM":"STR";
            printf(" %s:%s\n",attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,attrType);
         }
      }
      if(attrCatHeader.rblock==-1)
       {
       break;
       }
       else
       {
       header=attrCatHeader.rblock;
       }
      }
      printf("\n");
     
  }
}


