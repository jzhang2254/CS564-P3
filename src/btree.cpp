/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include <type_traits>
// #include "pagePtr.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	if (attrType != INTEGER) {
		std::cout<<"Not Implement"<<std::endl;
		return;
	}
	bufMgr = bufMgrIn;
	std::ostringstream idxStr;
  	idxStr << relationName << '.' << attrByteOffset;
  	outIndexName = idxStr.str(); // outIndexName is the name of the index file.
	Page* metaPage;
	PageId metaPageId = 1;
	IndexMetaInfo* metaData;
	bool fileExists = false;

  	try {
		// BlobFile temp = BlobFile::open(outIndexName);
		file = new BlobFile(outIndexName, false);
		// file = &temp; // already exists
		// Page* metaPage;
		bufMgr->readPage(file, metaPageId, metaPage);
		metaData = reinterpret_cast<IndexMetaInfo*>(metaPage);

		if (metaData->relationName != relationName || metaData->attrByteOffset != attrByteOffset || metaData->attrType != attrType) {
			bufMgr->unPinPage(file, metaPageId, false);
			throw BadIndexInfoException(outIndexName);
		}
  	} catch(FileNotFoundException e)
	{
		fileExists = true;
		file = new BlobFile(outIndexName, true);
	  	// Page* metaPage;
	  	// PageId metaPageId;
	  	bufMgr->allocPage(file, metaPageId, metaPage);
	  	metaData = reinterpret_cast<IndexMetaInfo*>(metaPage);
		strcpy(metaData->relationName, relationName.c_str());
		metaData->attrByteOffset = attrByteOffset;
		metaData->attrType = attrType;
		// construct root
		metaData->rootPageNo = createNonLeafInt(1);
		//construct first leaf node
		PageId leafPageId = createLeafInt();
		Page* rootPage;
		bufMgr->readPage(file, metaData->rootPageNo, rootPage);
		NonLeafNodeInt* root = reinterpret_cast<NonLeafNodeInt*>(rootPage);
		root->pageNoArray[0] = leafPageId;
		
		bufMgr->unPinPage(file, metaData->rootPageNo, true);  // un pin meta page ?????

	}	
	// build BTreeIndex object
	headerPageNum = 1;
	rootPageNum = metaData->rootPageNo;
	leafOccupancy = INTARRAYLEAFSIZE;
	nodeOccupancy = INTARRAYNONLEAFSIZE;
	
	scanExecuting = false;
	nextEntry = 0;
	currentPageNum = Page::INVALID_NUMBER;
	currentPageData = nullptr; 
	bufMgr->unPinPage(file, metaPageId, metaPage);
  // read inputs from fscan and insert into B tree

		FileScan fscan = FileScan(relationName, bufMgrIn); 
		try{
				RecordId scanRid;
				while(1)
				{
					fscan.scanNext(scanRid);
					std::string recordStr = fscan.getRecord();
					const char *record = recordStr.c_str();
					int key = *(int* )(record + attrByteOffset);
					insertEntryInt(key, scanRid);
					
				}
			}
			catch(const EndOfFileException &e)
			{
				std::cout << "Inserted all records" << std::endl;
			}	
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	if (currentPageNum != Page::INVALID_NUMBER) bufMgr->unPinPage(file, currentPageNum, true);
	bufMgr->flushFile(BTreeIndex::file);
	delete file;
	file = nullptr;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

PageId BTreeIndex::createNonLeafInt(int level) {
	Page* page;
	PageId pageId;
	bufMgr->allocPage(file, pageId, page);
	NonLeafNodeInt* node = reinterpret_cast<NonLeafNodeInt*>(page);
	node->level = level;
	for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) node->keyArray[i] = INT32_MAX;
	node->keyArray[0] = INT32_MAX;
	node->pageNoArray[0] = Page::INVALID_NUMBER;
	bufMgr->unPinPage(file, pageId, true);
	return pageId;
}

PageId BTreeIndex::createLeafInt() {
	PageId pageId;
	Page* page; //create new leaf node, key & rid are first things in page
	bufMgr->allocPage(file, pageId, page);
	LeafNodeInt* node = reinterpret_cast<LeafNodeInt*>(page);
	for (int i = 0; i < INTARRAYLEAFSIZE; i++) node->keyArray[i] = INT32_MAX;
	node->rightSibPageNo = Page::INVALID_NUMBER;
	bufMgr->unPinPage(file, pageId, true);
	return pageId;
}


void BTreeIndex::insertLeafInt(int &key, const RecordId rid, PageId &pageId) {	
	if (pageId == Page::INVALID_NUMBER) { // first entry -- ?? need this ??
		pageId = createLeafInt();
		Page* page; //create new leaf node, key & rid are first things in page
		bufMgr->readPage(file, pageId, page);
		LeafNodeInt* node = reinterpret_cast<LeafNodeInt*>(page);
		node->keyArray[0] = key;
		node->ridArray[0] = rid;
		bufMgr->unPinPage(file, pageId, true);
		return;
	}

	Page* page;
	bufMgr->readPage(file, pageId, page);
	LeafNodeInt* node = reinterpret_cast<LeafNodeInt*>(page);
	if (node->keyArray[INTARRAYLEAFSIZE-1] != INT32_MAX) {
		// split (leaf in half)
		Page* newPage;
		int mid = INTARRAYLEAFSIZE / 2;
		PageId newPageId = createLeafInt();
		bufMgr->readPage(file, newPageId, newPage);
		LeafNodeInt* newNode = reinterpret_cast<LeafNodeInt*>(newPage);

		for (int i = mid; i < INTARRAYLEAFSIZE; i++){
			newNode->keyArray[i-mid] = node->keyArray[i];
			newNode->ridArray[i-mid] = node->ridArray[i];
			node->keyArray[i] = INT32_MAX;
		}
		newNode->rightSibPageNo = node->rightSibPageNo;
		node->rightSibPageNo = newPageId;
		
		// insert into correct half
		if (key > newNode->keyArray[0]) {
			PageId tempPageId = newPageId;
			insertLeafInt(key, rid, tempPageId);
		} else {
			PageId tempPageId = pageId;
			insertLeafInt(key, rid, tempPageId);
		}
		bufMgr->unPinPage(file, pageId, true);
		pageId = newPageId;
		key = newNode->keyArray[0];
		bufMgr->unPinPage(file, newPageId, true);
		 
		return;
	}
	
	int i;
	for (i = 0; key > node->keyArray[i]; i++) ; //find insertion index

	if (node->keyArray[i] == INT32_MAX){ // insert in current node
		node->keyArray[i] = key;
		node->ridArray[i] = rid;
		bufMgr->unPinPage(file, pageId, true);
		pageId = Page::INVALID_NUMBER;
		return;
	} else { // shift to insert in middle of node
		int j; // last item
		for (j = INTARRAYLEAFSIZE-1; node->keyArray[j] == INT32_MAX; j--);
		for (; j >= i; j--) {
			node->keyArray[j+1] = node->keyArray[j];
			node->ridArray[j+1] = node->ridArray[j];
		}
		node->keyArray[i] = key;
		node->ridArray[i] = rid;
		bufMgr->unPinPage(file, pageId, true);
		pageId = Page::INVALID_NUMBER;
		return;
	}
}

void BTreeIndex::insertNoSplit(NonLeafNodeInt* node, const int newKey, const PageId newPageId) {
	int i;
	for (i = 0; i < INTARRAYNONLEAFSIZE && newKey > node->keyArray[i]; i++);
	if (node->keyArray[i] == INT32_MAX) {
			// insert without shift
		node->keyArray[i] = newKey;
		node->pageNoArray[i+1] = newPageId;
			
	} else { // shift and insert
			// keys
		int j;
		for (j = INTARRAYNONLEAFSIZE-1; node->keyArray[j] == INT32_MAX; j--);
		for (; j >= i; j--) {
			node->keyArray[j+1] = node->keyArray[j];
			node->pageNoArray[j+2] = node->pageNoArray[j+1];
		}
		node->keyArray[i] = newKey;
		node->pageNoArray[i+1] = newPageId;

	}
}

void BTreeIndex::insertNonLeafInt(int &key, const RecordId rid, PageId &pageId) {
	PageId result = Page::INVALID_NUMBER;
	if (pageId == Page::INVALID_NUMBER) { // create new page
		result = pageId = createNonLeafInt(1);
		
	}
	
	Page* currPage;
	bufMgr->readPage(file, pageId, currPage); // read current node
	NonLeafNodeInt* node = reinterpret_cast<NonLeafNodeInt*>(currPage);
	int i;
	for (i = 0; i < INTARRAYNONLEAFSIZE && key > node->keyArray[i]; i++) ; //find index
	
	PageId newPageId = node->pageNoArray[i];
	if (node->level == 0) {
		insertNonLeafInt(key, rid, newPageId);
	} else {
		insertLeafInt(key, rid, newPageId);

	} 
	Page* splitPage;
	PageId splitPageId;
	if (newPageId != Page::INVALID_NUMBER) {
		if (node->keyArray[INTARRAYNONLEAFSIZE-1] != INT32_MAX) {
			// split
			splitPageId = createNonLeafInt(node->level);
			bufMgr->readPage(file, splitPageId, splitPage);
			NonLeafNodeInt* newNode = reinterpret_cast<NonLeafNodeInt*>(splitPage);

			int mid = INTARRAYNONLEAFSIZE / 2;
			int newKey = node->keyArray[mid];  // key to be passed up
			node->keyArray[mid] = INT32_MAX;
			int j;
			for (j = mid+1; j < INTARRAYNONLEAFSIZE; j++){
				newNode->keyArray[(j-mid)-1] = node->keyArray[j];
				node->keyArray[j] = INT32_MAX;
			}
			for (j = mid+1; j <= INTARRAYNONLEAFSIZE; j++) {
				newNode->pageNoArray[(j-mid)-1] = node->pageNoArray[j];
				node->pageNoArray[j] = Page::INVALID_NUMBER;
			}
			if (key > newNode->keyArray[0]) {
				insertNoSplit(newNode, key, newPageId);
			} else {
				insertNoSplit(node, key, pageId);
			}
			pageId = splitPageId;
			key = newKey;
			bufMgr->unPinPage(file, splitPageId, true);
			bufMgr->unPinPage(file, pageId, true);
			return;
		}

		insertNoSplit(node, key, newPageId);
		bufMgr->unPinPage(file, pageId, true);
		// bufMgr->unPinPage(file, newPageId, true);
		pageId = Page::INVALID_NUMBER;
		
		return ;
	}
	bufMgr->unPinPage(file, pageId, true);
	pageId = Page::INVALID_NUMBER;
	return;
}

void BTreeIndex::insertEntryInt(const int key, const RecordId rid) 
{
	int newKey = key;
	PageId pageId = rootPageNum;
	insertNonLeafInt(newKey, rid, pageId);
	if (pageId != Page::INVALID_NUMBER) { // new root created
		PageId newRootPageId = createNonLeafInt(0);
		Page* rootPage;
		bufMgr->readPage(file, newRootPageId, rootPage);
		NonLeafNodeInt* root = reinterpret_cast<NonLeafNodeInt*>(rootPage);
		root->keyArray[0] = newKey;
		root->pageNoArray[0] = rootPageNum;
		root->pageNoArray[1] = pageId;
		rootPageNum  = newRootPageId;
		bufMgr->unPinPage(file, newRootPageId, true);

		// update meta
		Page* metaPage;
		bufMgr->readPage(file, headerPageNum, metaPage);
		IndexMetaInfo* metaData = reinterpret_cast<IndexMetaInfo*>(metaPage);
		metaData->rootPageNo = rootPageNum;
		bufMgr->unPinPage(file, headerPageNum, true);
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    if ((lowOpParm != GT && lowOpParm != GTE) || (highOpParm != LT && highOpParm != LTE))
        throw BadOpcodesException();

    lowOp = lowOpParm;
    highOp = highOpParm;
    lowValInt = *((int*) lowValParm);
    highValInt = *((int*) highValParm);

    if (lowValInt > highValInt)
        throw BadScanrangeException();

	scanExecuting = true;

    Page* leafPage;
	PageId leafPageId;
	Page* rootPage;
	bufMgr->readPage(file, rootPageNum, rootPage);
	traverse(rootPage, ((NonLeafNodeInt*) rootPage)->level, lowValParm, leafPageId);
	bufMgr->unPinPage(file, rootPageNum, false);
	bufMgr->readPage(file, leafPageId, leafPage);

	LeafNodeInt* leaf = reinterpret_cast<LeafNodeInt*>(leafPage);
    currentPageNum = leafPageId;

	bool foundFlag = false;
	while(!foundFlag) {
		
		for(int i = 0; i < leafOccupancy; i++) {
 
			if(leaf->keyArray[i] != INT32_MAX && 
				((lowOp == GT && leaf->keyArray[i] > lowValInt) || (lowOp == GTE && leaf->keyArray[i] >= lowValInt)) && 
				((highOp == LT && leaf->keyArray[i] < highValInt) || (highOp == LTE && leaf->keyArray[i] <= highValInt))) {
						
                currentPageData = leafPage;
				currentPageNum = leafPageId;
				nextEntry = i;
				foundFlag = true;
				break;
			} 
		}

		if(!foundFlag) {
			if(leaf->rightSibPageNo != NULL) {
				PageId nextPageId = leaf->rightSibPageNo;
				bufMgr->unPinPage(file, leafPageId, false);
				bufMgr->readPage(file, nextPageId, leafPage);
					
				leafPageId = nextPageId;
				leaf = (LeafNodeInt*) leafPage;
			} else {
				bufMgr->unPinPage(file, leafPageId, false);

				nextEntry = 0;
				foundFlag = true; 
                throw NoSuchKeyFoundException();
            }
        }
    }

	
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
	if (!scanExecuting) {
        throw ScanNotInitializedException();
    }

    LeafNodeInt* currNode = reinterpret_cast<LeafNodeInt*>(currentPageData);
    if (currNode->keyArray[nextEntry] == INT32_MAX or nextEntry == leafOccupancy) {
        // found page to read; look for sibling
        if (currNode->rightSibPageNo == 0) {
            throw IndexScanCompletedException();
        }

        // manage buffer
        bufMgr->unPinPage(file, currentPageNum, false);
        currentPageNum = currNode->rightSibPageNo;
        bufMgr->readPage(file, currNode->rightSibPageNo, currentPageData);
        currNode = (LeafNodeInt*)currentPageData;
        nextEntry = 0;
    }
        // check for matching rid
    int key = currNode->keyArray[nextEntry];
	bool match;
	if (lowOp== GTE && highOp == LTE) {
	   match = (key <= highValInt && key >= lowValInt);
    } else if (lowOp == GTE && highOp == LT) {
           match = (key < highValInt && key >= lowValInt);
	} else if (lowOp == GT && highOp == LTE) {
           match = (key <= highValInt && key > lowValInt);
	} else { // GT, LT
	   match = (key < highValInt && key > lowValInt);
	}

	if (match) {
       outRid = currNode->ridArray[nextEntry];
	   nextEntry++;
	} else {
           throw IndexScanCompletedException();
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
    if (scanExecuting == false)
        throw ScanNotInitializedException();
    
    scanExecuting = false;

    if (currentPageNum != Page::INVALID_NUMBER) {
		std::cout<<currentPageNum<<std::endl;
        bufMgr->unPinPage(file, currentPageNum, false);
		currentPageNum = Page::INVALID_NUMBER;
	}
}

void BTreeIndex::traverse(Page* page, int pageLevel, const void* keyPtr, PageId &leafID) {

	int index;

    if(pageLevel == 0) {
        int key = *(int*) keyPtr;
		NonLeafNodeInt* nodeInt = (NonLeafNodeInt*) page;

		for(int i = 0; i < nodeOccupancy; i++) {
			if(key < nodeInt->keyArray[0]) {
				index = 0; 
			}
			else if(key >= nodeInt->keyArray[i] && !(i == nodeOccupancy - 1 || nodeInt->keyArray[i+1] == INT32_MAX) && key < nodeInt->keyArray[i + 1]) {
				index = i + 1;
			}
			else if(key >= nodeInt->keyArray[i] && (i == nodeOccupancy - 1 || nodeInt->keyArray[i + 1] == INT32_MAX)) {
				index = i + 1;
			}
		}

		Page* child;
		bufMgr->readPage(file, ((NonLeafNodeInt*) page)->pageNoArray[index], child);
		traverse(child, ((NonLeafNodeInt*) child)->level, keyPtr, leafID);

		bufMgr->unPinPage(file, ((NonLeafNodeInt*) page)->pageNoArray[index], false);
	} else {
		int key = *((int*) keyPtr);
		NonLeafNodeInt* nodeInt = (NonLeafNodeInt*) page;

		for(int i = 0; i < nodeOccupancy; i++) {
			if(key < nodeInt->keyArray[0]) {
				index = 0; 
			}
			else if(key >= nodeInt->keyArray[i] && !(i == nodeOccupancy - 1 || nodeInt->keyArray[i+1] == INT32_MAX) && key < nodeInt->keyArray[i + 1]) {
				index = i + 1;
			}
			else if(key >= nodeInt->keyArray[i] && (i == nodeOccupancy - 1 || nodeInt->keyArray[i + 1] == INT32_MAX)) {
				index = i + 1;
			}
		}
        
		leafID = ((NonLeafNodeInt*) page)->pageNoArray[index];
	}

}

}