
#ifndef CEPH_RGWDIRECTORY_H
#define CEPH_RGWDIRECTORY_H

#include <stdlib.h>
#include <sys/types.h>
#include <sstream>


#include <string>
#include <iostream>
#include <vector>
#include <list>

using namespace std;

/* the metadata which is written to the directory
 * you can add or remove some of the fields based on
 * your required caching policy
 */
typedef struct objDirectoryStruct {
    string key; //bucketID_ObjectID
    string owner;
    string location;
    uint8_t dirty;
    uint64_t size;
    string createTime;
    string lastAccessTime;
    string etag;
    string backendProtocol;
    string bucket_name;
    string obj_name;
}objectDirectoryStruct_t;

typedef struct blockDirectoryStruct {
    string key; //bucketID_ObjectID_offset
    string owner;
    string location;
    string size;
    string createTime;
    string lastAccessTime;
    string etag;
	std::vector<std::pair<std::string, std::string>> popularTenants;
}blockDirectoryStruct_t;

typedef struct cacheStatDirectoryStruct {
    uint64_t hitCount;
    uint64_t reqCount;
    uint64_t capacity;
    string ID;
}cacheStatDirectoryStruct_t;

class RGWDirectory{
public:
  RGWDirectory() {}
  ~RGWDirectory() {}
  virtual int getValue(void *ptr);
  virtual int setValue(void *ptr);
};

class RGWObjectDirectory: RGWDirectory {
public:
  //int getValue(objectDirectory_t &dir_val);
  int getValue(void *ptr);
  //int setValue(string key, string timeStr, string bucket_name, string obj_name, string location, string owner, uint64_t obj_size, string etag);
  int setValue(void *ptr);
  //std::vector<std::pair<std::string, std::string>> get_aged_keys(string startTime, string endTime);
	
};



#endif
