#include "AttrCacheTable.h"
#include<iostream>
#include <cstring>

AttrCacheEntry *AttrCacheTable::attrCache[MAX_OPEN];
/* returns the attribute with name `attrName` for the relation corresponding to relId
NOTE: this function expects the caller to allocate memory for `*attrCatBuf`
*/
int AttrCacheTable::getAttrCatEntry(int recId, char attrName[ATTR_SIZE], AttrCatEntry *attrCatBuf) {

    // check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
    if( recId <0 || recId >= MAX_OPEN) {

        return E_OUTOFBOUND;
    }

     // check if attrCache[relId] == nullptr and return E_RELNOTOPEN if true
    if(attrCache[recId] == nullptr) {

        return E_RELNOTOPEN;
    }
    
     // traverse the linked list of attribute cache entries
    for(AttrCacheEntry *entry = attrCache[recId]; entry != nullptr; entry = entry->next) {

        if(strcmp(entry ->attrCatEntry.attrName ,attrName)==0) {

            *attrCatBuf = entry->attrCatEntry;// copy entry->attrCatEntry to *attrCatBuf and return SUCCESS;
            
            return SUCCESS;
        }
    }
    // there is no attribute for the relation
    return E_ATTRNOTEXIST;
}
int AttrCacheTable::getAttrCatEntry(int recId, int attrOffset, AttrCatEntry *attrCatBuf) {

    // check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
    if( recId <0 || recId >= MAX_OPEN) {

        return E_OUTOFBOUND;
    }

     // check if attrCache[relId] == nullptr and return E_RELNOTOPEN if true
    if(attrCache[recId] == nullptr) {

        return E_RELNOTOPEN;
    }
    
     // traverse the linked list of attribute cache entries
    for(AttrCacheEntry *entry = attrCache[recId]; entry != nullptr; entry = entry->next) {

        if(entry->attrCatEntry.offset == attrOffset) {

            *attrCatBuf = entry->attrCatEntry;// copy entry->attrCatEntry to *attrCatBuf and return SUCCESS;

            return SUCCESS;
        }
    }
    // there is no attribute for the relation
    return E_ATTRNOTEXIST;
}

/* Converts a attribute catalog record to AttrCatEntry struct
    We get the record as Attribute[] from the BlockBuffer.getRecord() function.
    This function will convert that to a struct AttrCatEntry type.
*/
void AttrCacheTable::recordToAttrCatEntry(union Attribute record[ATTRCAT_NO_ATTRS], 
                                                AttrCatEntry* attrCatEntry){
       strcpy(attrCatEntry->relName, record[ATTRCAT_REL_NAME_INDEX].sVal);
       strcpy(attrCatEntry->attrName, record[ATTRCAT_ATTR_NAME_INDEX].sVal);
       attrCatEntry->attrType = record[ATTRCAT_ATTR_TYPE_INDEX].nVal;
       attrCatEntry->primaryFlag = record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal;
       attrCatEntry->rootBlock = record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
       attrCatEntry->offset = record[ATTRCAT_OFFSET_INDEX].nVal;
       
}

int AttrCacheTable::resetSearchIndex(int relId, int attrOffset)
{
    IndexId temp;
    temp.block = -1;
    temp.index = -1;
    int ret = AttrCacheTable::setSearchIndex(relId,attrOffset,&temp);
    return ret;

}
int AttrCacheTable::resetSearchIndex(int relId, char attrName[ATTR_SIZE])
{
    IndexId temp;
    temp.block = -1;
    temp.index = -1;
    int ret = AttrCacheTable::setSearchIndex(relId,attrName,&temp);
    return ret;

}

void AttrCacheTable::attrCatEntryToRecord(AttrCatEntry *attrCatEntry, union Attribute record[ATTRCAT_NO_ATTRS]){
    strcpy(record[ATTRCAT_REL_NAME_INDEX].sVal,attrCatEntry->relName);
    strcpy(record[ATTRCAT_ATTR_NAME_INDEX].sVal,attrCatEntry->attrName);
    record[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrCatEntry->attrType;
    record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = attrCatEntry->primaryFlag;
    record[ATTRCAT_ROOT_BLOCK_INDEX].nVal = attrCatEntry->rootBlock;
    record[ATTRCAT_OFFSET_INDEX].nVal = attrCatEntry->offset;
   return ;
}
int AttrCacheTable::getSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId *searchIndex) {

  if(relId<0 || relId>=MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  if(attrCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }
  AttrCacheEntry *attrCacheEntry = attrCache[relId];
  while(attrCacheEntry != nullptr)
  {
    if (strcmp(attrCacheEntry->attrCatEntry.attrName,attrName) == 0)
    {
        *searchIndex = attrCacheEntry->searchIndex;
      
      return SUCCESS;
    }
  }

  return E_ATTRNOTEXIST;
}
int AttrCacheTable::setSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId *searchIndex) {

  if(relId<0 || relId>=MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  if(attrCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }

  AttrCacheEntry *attrCacheEntry = attrCache[relId];
  while(attrCacheEntry != nullptr)
  {
    if (strcmp(attrCacheEntry->attrCatEntry.attrName,attrName) == 0)
    {
      attrCacheEntry->searchIndex = *searchIndex;
      return SUCCESS;
    }
  }

  return E_ATTRNOTEXIST;
}
int AttrCacheTable::setSearchIndex(int relId, int attrOffset, IndexId *searchIndex) {

  if(relId<0 || relId>=MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  if(attrCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }

  AttrCacheEntry *attrCacheEntry = attrCache[relId];
  while(attrCacheEntry != nullptr)
  {
    if (attrCacheEntry->attrCatEntry.offset == attrOffset)
    {
      attrCacheEntry->searchIndex = *searchIndex;
      return SUCCESS;
    }
  }

  return E_ATTRNOTEXIST;
}
int AttrCacheTable::getSearchIndex(int relId, int attrOffset, IndexId *searchIndex) {

  if(relId<0 || relId>=MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  if(attrCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }
  AttrCacheEntry *attrCacheEntry = attrCache[relId];
  while(attrCacheEntry != nullptr)
  {
    if (attrCacheEntry->attrCatEntry.offset == attrOffset)
    {
      attrCacheEntry->searchIndex = *searchIndex;
      return SUCCESS;
    }
  }

  return E_ATTRNOTEXIST;
}
