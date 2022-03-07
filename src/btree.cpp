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
	std::cout<<outIndexName<<std::endl;
	Page* metaPage;
	PageId metaPageId;
	IndexMetaInfo* metaData;
	bool fileExists = true;
  	try {
		file = new BlobFile(outIndexName, false); // already exists
		// Page* metaPage;
		bufMgr->readPage(file, 1, metaPage);
		metaData = reinterpret_cast<IndexMetaInfo*>(metaPage);
		if (metaData->relationName != relationName || metaData->attrByteOffset != attrByteOffset || metaData->attrType != attrType) {
			throw BadIndexInfoException(outIndexName);
		}

  	} catch(FileNotFoundException e)
	{
		fileExists = false;
		file = new BlobFile(outIndexName, true);
	  	// Page* metaPage;
	  	PageId metaPageId;
	  	bufMgr->allocPage(file, metaPageId, metaPage);
	  	if (metaPageId != 1) std::cout<<"Expected metaPageId = 1"<<std::endl; // REMOVE DEBUG STATEMENT
	  	metaData = reinterpret_cast<IndexMetaInfo*>(metaPage);
		strcpy(metaData->relationName, relationName.c_str());
		metaData->attrByteOffset = attrByteOffset;
		metaData->attrType = attrType;
		// metaData->rootPageNo
		// construct root
		metaData->rootPageNo = createNonLeafInt(1);
		//construct first leaf node
		PageId leafPageId = createLeafInt();
		Page* rootPage;
		bufMgr->readPage(file, metaData->rootPageNo, rootPage);
		NonLeafNodeInt* root = reinterpret_cast<NonLeafNodeInt*>(rootPage);
		root->pageNoArray[0] = leafPageId;
		
		bufMgr->unPinPage(file, metaData->rootPageNo, true);
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
  
  // read inputs from fscan and insert into B tree
	if (fileExists) {
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
		fscan.~FileScan();

	}
	// bufMgr->flushFile(file);
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	scanExecuting = false; // cleanup state variables
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
	for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) node->keyArray[i] = INT_MAX;
	node->keyArray[0] = INT_MAX;
	node->pageNoArray[0] = Page::INVALID_NUMBER;
	bufMgr->unPinPage(file, pageId, true);
	return pageId;
}

PageId BTreeIndex::createLeafInt() {
	PageId pageId;
	Page* page; //create new leaf node, key & rid are first things in page
	bufMgr->allocPage(file, pageId, page);
	LeafNodeInt* node = reinterpret_cast<LeafNodeInt*>(page);
	for (int i = 0; i < INTARRAYLEAFSIZE; i++) node->keyArray[i] = INT_MAX;
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
	if (node->keyArray[INTARRAYLEAFSIZE-1] != INT_MAX) {
		// split (leaf in half)
		Page* newPage;
		int mid = INTARRAYLEAFSIZE / 2;
		PageId newPageId = createLeafInt();
		bufMgr->readPage(file, newPageId, newPage);
		LeafNodeInt* newNode = reinterpret_cast<LeafNodeInt*>(newPage);
		for (int i = mid; i < INTARRAYLEAFSIZE; i++){
			newNode->keyArray[i-mid] = node->keyArray[i];
			newNode->ridArray[i-mid] = node->ridArray[i];
			node->keyArray[i] = INT_MAX;
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

	if (node->keyArray[i] == INT_MAX){ // insert in current node
		node->keyArray[i] = key;
		node->ridArray[i] = rid;
		bufMgr->unPinPage(file, pageId, true);
		pageId = Page::INVALID_NUMBER;
		return;
	} else { // shift to insert in middle of node
		int j; // last item
		for (j = INTARRAYLEAFSIZE-1; node->keyArray[j] == INT_MAX; j--);
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
	if (node->keyArray[i] == INT_MAX) {
			// insert without shift
		node->keyArray[i] = newKey;
		node->pageNoArray[i+1] = newPageId;
			
	} else { // shift and insert
			// keys
		int j;
		for (j = INTARRAYNONLEAFSIZE-1; node->keyArray[j] == INT_MAX; j--);
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
	if (newPageId != Page::INVALID_NUMBER) {
		if (node->keyArray[INTARRAYNONLEAFSIZE-1] != INT_MAX) {
			// split
			Page* splitPage;
			PageId splitPageId = createNonLeafInt(node->level);
			bufMgr->readPage(file, splitPageId, splitPage);
			NonLeafNodeInt* newNode = reinterpret_cast<NonLeafNodeInt*>(splitPage);

			int mid = INTARRAYNONLEAFSIZE / 2;
			int newKey = node->keyArray[mid];  // key to be passed up
			node->keyArray[mid] = INT_MAX;
			int j;
			for (j = mid+1; j < INTARRAYNONLEAFSIZE; j++){
				newNode->keyArray[(j-mid)-1] = node->keyArray[j];
				node->keyArray[j] = INT_MAX;
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
			return;
		}

		insertNoSplit(node, key, newPageId);
		pageId = Page::INVALID_NUMBER;
		return ;
	}
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
		bufMgr->unPinPage(file, rootPageNum, true);

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
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
	if (!scanExecuting) {
        throw ScanNotInitializedException();
    }

    if (nextEntry == 0) {
        throw IndexScanCompletedException();
    }

    LeafNodeInt* currNode = (LeafNodeInt*)currentPageData;
    if (currNode->ridArray[nextEntry].page_number == 0 || nextEntry == leafOccupancy) {
        // found page to read; look for sibling
        if (currNode->rightSibPageNo == 0) {
            throw IndexScanCompletedException();
        }

        // manage buffer
        bufMgr->unPinPage(file, currentPageNum, false);
        // currentPageNum = currNode->rightSibPageNo;
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
	} else {
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

    if (currentPageNum != 0)
        bufMgr->unPinPage(file, currentPageNum, false);
}

}
