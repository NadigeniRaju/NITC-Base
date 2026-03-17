#include "OpenRelTable.h"
#include<stdio.h>
#include <stdlib.h>
#include <cstring>

 OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];
OpenRelTable::OpenRelTable() {

  // initialize relCache and attrCache with nullptr
  for (int i = 0; i < MAX_OPEN; ++i) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
    tableMetaInfo[i].free = true;
  }

  /************ Setting up Relation Cache entries ************/
  // (we need to populate relation cache with entries for the relation catalog
  //  and attribute catalog.)

  /**** setting up Relation Catalog relation in the Relation Cache Table****/
  RecBuffer relCatBlock(RELCAT_BLOCK);

  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

  struct RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;
  relCacheEntry.searchIndex = {-1,-1};

  // allocate this on the heap because we want it to persist outside this function
  RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;

  
  /**** setting up Attribute Catalog relation in the Relation Cache Table ****/

  // set up the relation cache entry for the attribute catalog similarly
  // from the record at RELCAT_SLOTNUM_FOR_ATTRCAT

  // set the value at RelCacheTable::relCache[ATTRCAT_RELID]


  /************ Setting up Attribute cache entries ************/
  // (we need to populate attribute cache with entries for the relation catalog
  //  and attribute catalog.)

  /**** setting up Relation Catalog relation in the Attribute Cache Table ****/
  Attribute relCatRecord1[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord1,RELCAT_SLOTNUM_FOR_ATTRCAT);

  RelCacheEntry relCacheEntry1;
  RelCacheTable::recordToRelCatEntry(relCatRecord1,&relCacheEntry1.relCatEntry);
  relCacheEntry1.recId.block = RELCAT_BLOCK;
  relCacheEntry1.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;
  relCacheEntry1.searchIndex = {-1,-1};

   RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
   *(RelCacheTable::relCache[ATTRCAT_RELID]) = relCacheEntry1;


  RecBuffer attrCatBlock(ATTRCAT_BLOCK);

  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

  // iterate through all the attributes of the relation catalog and create a linked
  // list of AttrCacheEntry (slots 0 to 5)
  // for each of the entries, set
  //    attrCacheEntry.recId.block = ATTRCAT_BLOCK;
  //    attrCacheEntry.recId.slot = i   (0 to 5)
  //    and attrCacheEntry.next appropriately
  // NOTE: allocate each entry dynamically using malloc

  // set the next field in the last entry to nullptr
  AttrCacheEntry *head = NULL,*tail = NULL,*temp;
  for(int i = 0;i <= 5; i++) {

    attrCatBlock.getRecord(attrCatRecord,i);

    AttrCacheEntry attrCacheEntry;
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord,&attrCacheEntry.attrCatEntry);
    attrCacheEntry.recId.block = ATTRCAT_BLOCK;
     attrCacheEntry.recId.slot = i;
     attrCacheEntry.next = NULL;
     temp = (struct AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
     (*temp) = attrCacheEntry;
     if(head == NULL)
     {
         head = temp;
         tail = temp;
     }
     else {
         tail->next = temp;
         tail = temp;
     }
     
  }

  AttrCacheTable::attrCache[RELCAT_RELID] =  head/* head of the linked list */;

  /**** setting up Attribute Catalog relation in the Attribute Cache Table ****/

  // set up the attributes of the attribute cache similarly.
  // read slots 6-11 from attrCatBlock and initialise recId appropriately

  // set the value at AttrCacheTable::attrCache[ATTRCAT_RELID]
  head = NULL,tail = NULL;
  for(int i = 6;i <= 11; i++) {

    attrCatBlock.getRecord(attrCatRecord,i);

    AttrCacheEntry attrCacheEntry;
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord,&attrCacheEntry.attrCatEntry);
    attrCacheEntry.recId.block = ATTRCAT_BLOCK;
     attrCacheEntry.recId.slot = i;
     attrCacheEntry.next = NULL;
     temp = (struct AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
     (*temp) = attrCacheEntry;
     if(head == NULL)
     {
         head = temp;
         tail = temp;
     }
     else {
         tail->next = temp;
         tail = temp;
     }
     
  }

   AttrCacheTable::attrCache[ATTRCAT_RELID] =  head/* head of the linked list */;



   //stage5
    /************ Setting up tableMetaInfo entries ************/
    tableMetaInfo[RELCAT_RELID].free = false;
    strcpy(tableMetaInfo[RELCAT_RELID].relName,"RELATIONCAT");

    tableMetaInfo[ATTRCAT_RELID].free = false;
    strcpy(tableMetaInfo[ATTRCAT_RELID].relName,"ATTRIBUTECAT");

}

OpenRelTable::~OpenRelTable() {
  // free all the memory that you allocated in the constructor
  for(int i =2; i<MAX_OPEN;++i) {
    if(!tableMetaInfo[i].free) {
      OpenRelTable::closeRel(i);
    }
  }
   /**** Closing the catalog relations in the relation cache ****/

    //releasing the relation cache entry of the attribute catalog

    if (RelCacheTable::relCache[ATTRCAT_RELID]->dirty == true) {

        /* Get the Relation Catalog entry from RelCacheTable::relCache
        Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
        // declaring an object of RecBuffer class to write back to the buffer
        Attribute record[RELCAT_NO_ATTRS];
        RelCatEntry relCatEntry = RelCacheTable::relCache[ATTRCAT_RELID]->relCatEntry;
        RelCacheTable::relCatEntryToRecord(&relCatEntry,record);
        RecId relId = RelCacheTable::relCache[ATTRCAT_RELID]->recId;
        RecBuffer relCatBlock(relId.block);
        relCatBlock.setRecord(record,relId.slot);

        // Write back to the buffer using relCatBlock.setRecord() with recId.slot
    }
    // free the memory dynamically allocated to this RelCacheEntry
    free(RelCacheTable::relCache[ATTRCAT_RELID]);


    //releasing the relation cache entry of the relation catalog

    if(RelCacheTable::relCache[RELCAT_RELID]->dirty == true) {

        /* Get the Relation Catalog entry from RelCacheTable::relCache
        Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */

        // declaring an object of RecBuffer class to write back to the buffer
         Attribute record[RELCAT_NO_ATTRS];
        RelCatEntry relCatEntry = RelCacheTable::relCache[RELCAT_RELID]->relCatEntry;
        RelCacheTable::relCatEntryToRecord(&relCatEntry,record);
        RecId relId = RelCacheTable::relCache[RELCAT_RELID]->recId;
        RecBuffer relCatBlock(relId.block);
        relCatBlock.setRecord(record,relId.slot);

        // Write back to the buffer using relCatBlock.setRecord() with recId.slot
    }
    // free the memory dynamically allocated for this RelCacheEntry
    free(RelCacheTable::relCache[RELCAT_RELID]);


     /************ Closing the catalog relations in the attribute cache ************/

    /****** releasing the entry corresponding to Attribute Catalog relation from Attribute Cache Table ******/

    // for all the entries in the linked list of the ATTRCAT_RELIDth Attribute Cache entry.
    AttrCacheEntry *attrRelEntry = AttrCacheTable::attrCache[ATTRCAT_RELID];
    while(attrRelEntry!=NULL)
    {
        if (attrRelEntry->dirty == true)
        {
            /* Get the Attribute Catalog entry from Cache using AttrCacheTable::attrCatEntryToRecord().
            Write back that entry by instantiating RecBuffer class. Use recId member
            field and recBuffer.setRecord() */
            Attribute record[ATTRCAT_NO_ATTRS];
            AttrCacheTable::attrCatEntryToRecord(&(attrRelEntry->attrCatEntry),record);
            RecBuffer recBuffer(attrRelEntry->recId.block);
            recBuffer.setRecord(record,attrRelEntry->recId.slot);

        }
         AttrCacheEntry *temp = attrRelEntry;
         attrRelEntry = attrRelEntry->next;
        // free the memory dynamically alloted to this entry in Attribute Cache linked list.
        free(temp);
    }

    /****** releasing the entry corresponding to Relation Catalog relation from Attribute Cache Table ******/

    // for all the entries in the linked list of the RELCAT_RELIDth Attribute Cache entry.
     AttrCacheEntry *RelCatRelEntry = AttrCacheTable::attrCache[RELCAT_RELID];
    while(RelCatRelEntry!=NULL)
    {
        if (RelCatRelEntry->dirty == true) {

            /* Get the Attribute Catalog entry from Cache using AttrCacheTable::attrCatEntryToRecord().
            Write back that entry by instantiating RecBuffer class. Use recId
            member field and recBuffer.setRecord() */
             Attribute record[ATTRCAT_NO_ATTRS];
            AttrCacheTable::attrCatEntryToRecord(&(RelCatRelEntry->attrCatEntry),record);
            RecBuffer recBuffer(RelCatRelEntry->recId.block);
            recBuffer.setRecord(record,RelCatRelEntry->recId.slot);
        }
         AttrCacheEntry * temp = RelCatRelEntry;
         RelCatRelEntry = RelCatRelEntry->next;
        // free the memory dynamically alloted to this entry in Attribute Cache linked list.
        free(temp);
    }
}

//stage4
/* This function will open a relation having name `relName`.
Since we are currently only working with the relation and attribute catalog, we
will just hardcode it. In subsequent stages, we will loop through all the relations
and open the appropriate one.
*/
int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {

  for(int i =0;i<MAX_OPEN;i++) {
     if(!tableMetaInfo[i].free && strcmp(tableMetaInfo[i].relName,relName) == 0)
     return i;
  }
  
  return E_RELNOTOPEN;
}

//stage5

int OpenRelTable::getFreeOpenRelTableEntry() {

  for(int i = 0;i<MAX_OPEN;i++) {
    if(tableMetaInfo[i].free)
     return i;
  }
  return E_CACHEFULL;
}

int OpenRelTable::openRel(char relName[ATTR_SIZE]) {

  /* the relation `relName` already has an entry in the Open Relation Table */
    // (checked using OpenRelTable::getRelId())
  int relId = OpenRelTable::getRelId(relName);
  if(relId >=0 && relId<MAX_OPEN) {

    return relId; // return that relation id;
  }

  /* find a free slot in the Open Relation Table
     using OpenRelTable::getFreeOpenRelTableEntry(). */
  int freeSlot = OpenRelTable::getFreeOpenRelTableEntry();

  /* free slot not available */
  if( freeSlot  == E_CACHEFULL) {
    return E_CACHEFULL;
  }
  // let relId be used to store the free slot.
  relId = freeSlot;


      /****** Setting up Relation Cache entry for the relation ******/

      /*Care should be taken to reset the searchIndex of the relation RELCAT_RELID
      before calling linearSearch().*/

      RelCacheTable::resetSearchIndex(RELCAT_RELID);
      Attribute relname;
      strcpy(relname.sVal,relName);
      char attributeRelName[10];
      strcpy(attributeRelName, "RelName");
    RecId relCatRecId = BlockAccess::linearSearch(RELCAT_RELID,attributeRelName,relname,EQ);

  if(relCatRecId.block == -1 && relCatRecId.slot == -1) {
    // (the relation is not found in the Relation Catalog.)
    return E_RELNOTEXIST;
  }

  RecBuffer relCatBlock(relCatRecId.block);
  Attribute record[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(record,relCatRecId.slot);
  struct RelCacheEntry *relCacheEntry = (struct RelCacheEntry *)malloc(sizeof(struct RelCacheEntry));
  RelCacheTable::recordToRelCatEntry(record,&(relCacheEntry->relCatEntry));
  relCacheEntry->recId.block = relCatRecId.block; 
  relCacheEntry->recId.slot = relCatRecId.slot;
  relCacheEntry->searchIndex.block = -1;
  relCacheEntry->searchIndex.slot  = -1;
  relCacheEntry->dirty = false;
  RelCacheTable::relCache[relId]=relCacheEntry;
  


  /****** Setting up Attribute Cache entry for the relation ******/
  AttrCacheEntry *listHead = nullptr,*listTail = nullptr,*temp;
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    while(true){
       RecId attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID,(char *)"RelName",relname,EQ);
       if(attrcatRecId.block == -1 && attrcatRecId.slot == -1)
       break;

       
      RecBuffer attrCatBuffer(attrcatRecId.block);
      Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

      attrCatBuffer.getRecord(attrCatRecord,attrcatRecId.slot);

       AttrCacheEntry attrCacheEntry;
      AttrCacheTable::recordToAttrCatEntry(attrCatRecord,&attrCacheEntry.attrCatEntry);
      attrCacheEntry.recId.block = attrcatRecId.block;
     attrCacheEntry.recId.slot = attrcatRecId.slot;
     attrCacheEntry.next = NULL;
     temp = (struct AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
     (*temp) = attrCacheEntry;
     temp->dirty = false;
      
      RelCacheTable::setSearchIndex(ATTRCAT_RELID,&attrcatRecId);
      if(listHead == nullptr) {
        listHead = temp;
        listTail = temp;
      }
      else
      {
        listTail->next = temp;
        listTail = temp;
      }

    }
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    AttrCacheTable::attrCache[relId]=listHead;

     /****** Setting up metadata in the Open Relation Table for the relation******/
     tableMetaInfo[relId].free = false;
     strcpy(tableMetaInfo[relId].relName,relName);

     return relId;

}



int OpenRelTable::closeRel(int relId) {
  /* rel-id corresponds to relation catalog or attribute catalog*/
  if (relId ==RELCAT_RELID || relId ==ATTRCAT_RELID) {
    return E_NOTPERMITTED;
  }
  /* 0 <= relId < MAX_OPEN */
  if (relId<0 || relId>=MAX_OPEN) {
    return E_OUTOFBOUND;
  }
  /* rel-id corresponds to a free slot*/
  if (tableMetaInfo[relId].free == true) {
    return E_RELNOTOPEN;
  }

  /****** Releasing the Relation Cache entry of the relation ******/

    /* RelCatEntry of the relId-th Relation Cache entry has been modified */
   if(RelCacheTable::relCache[relId]->dirty == true)
   {
    /* Get the Relation Catalog entry from RelCacheTable::relCache
    Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
        Attribute record[RELCAT_NO_ATTRS];
        RelCacheTable::relCatEntryToRecord(&RelCacheTable::relCache[relId]->relCatEntry,record);
         // declaring an object of RecBuffer class to write back to the buffer
         RecBuffer relCatBlock(RelCacheTable::relCache[relId]->recId.block);
         relCatBlock.setRecord(record,RelCacheTable::relCache[relId]->recId.slot);

   }
    
  // free the memory allocated in the relation and attribute caches which was
  // allocated in the OpenRelTable::openRel() function
  free(RelCacheTable::relCache[relId]);
      AttrCacheEntry *curr = AttrCacheTable::attrCache[relId];
      while (curr != nullptr) {
          // Write back to disk if modified
        if (curr->dirty == true) {
            Attribute record[ATTRCAT_NO_ATTRS];
            AttrCacheTable::attrCatEntryToRecord(&(curr->attrCatEntry), record);
            RecBuffer attrCatBlock(curr->recId.block);
            attrCatBlock.setRecord(record, curr->recId.slot);
          }
          AttrCacheEntry *next = curr->next;
          free(curr);
          curr = next;
      }

  tableMetaInfo[relId].free = true;


  // update `tableMetaInfo` to set `relId` as a free slot
  // update `relCache` and `attrCache` to set the entry at `relId` to nullptr
  RelCacheTable::relCache[relId] = nullptr;
  AttrCacheTable::attrCache[relId] = nullptr;

  return SUCCESS;
}
