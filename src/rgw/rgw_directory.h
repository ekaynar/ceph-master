
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
struct directory_values {
    string key; //bucketID_ObjectID
    string owner;
    string time;
    string bucket_name;
    string obj_name;
    string location;
    uint64_t obj_size;
    string etag;
};

class RGWDirectory{
public:
  RGWDirectory() {}
  ~RGWDirectory() {}
  int getMetaValue(directory_values &dir_val);
  int setMetaValue(string key, string timeStr, string bucket_name, string obj_name, string location, string owner, uint64_t obj_size, string etag);
  std::vector<std::pair<std::string, std::string>> get_aged_keys(string startTime, string endTime);
};



#endif
