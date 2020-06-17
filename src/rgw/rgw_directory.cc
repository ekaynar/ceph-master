


#include <cpp_redis/cpp_redis>
#include "rgw_directory.h"
#include <string>
#include <iostream>
#include <vector>
#include <list>


//int RGWDirectory::setValue(string key, string timeStr, string bucket_name, string obj_name, string location, string owner, uint64_t obj_size, string etag){
int RGWObjectDirectory::setValue(void *ptr){

  objectDirectory_t *objDir = (objectDirectory_t *)ptr;
  cpp_redis::client client;
  //  //client.connect();
  client.connect("127.0.0.1", 7000);

  //dout(10) << __func__ << " redis set key ="  << key << dendl;
  std::vector<std::pair<std::string, std::string>> list;
  std::vector<std::string> keys;
  std::multimap<std::string, std::string> timeKey;
  std::vector<std::string> options;

  //creating a list of key's properties
  list.push_back(std::make_pair("key", objDir->key));
  list.push_back(std::make_pair("owner", objDir->owner));
  list.push_back(std::make_pair("location", objDir->location));
  list.push_back(std::make_pair("dirty", std::to_string(objDir->dirty)));
  list.push_back(std::make_pair("size", std::to_string(objDir->size)));
  list.push_back(std::make_pair("createTime", objDir->createTime));
  list.push_back(std::make_pair("lastAccessTime", objDir->lastAccessTime));
  list.push_back(std::make_pair("etag", objDir->etag));
  list.push_back(std::make_pair("backendProtocol", objDir->backendProtocol));
  list.push_back(std::make_pair("bucket_name", objDir->bucket_name));
  list.push_back(std::make_pair("obj_name", objDir->obj_name));

  //creating a key entry
  keys.push_back(objDir->key);

  //making key and time a pair
  timeKey.emplace(objDir->createTime,objDir->key);


  client.hmset(objDir->key, list, [](cpp_redis::reply &reply){
  });

  client.rpush("objectDirectory", keys, [](cpp_redis::reply &reply){
  });


  client.zadd("keyDirectory", options, timeKey, [](cpp_redis::reply &reply){
  });

  // synchronous commit, no timeout
  client.sync_commit();
  return 0;

}


int RGWDirectory::getValue(void *ptr){

  objectDirectory_t *objDir = (objectDirectory_t *) ptr;

    string key;
    string owner;
    string location;
    string dirty;
    string size;
    string createTime;
    string lastAccessTime;
    string etag;
    string backendProtocol;
    string bucket_name;
    string obj_name;

	uint64_t obj_size;
	uint8_t obj_dirty;

  cpp_redis::client client;
  client.connect("127.0.0.1", 7000);

  std::vector<std::string> fields;
  fields.push_back("key");
  fields.push_back("owner");
  fields.push_back("location");
  fields.push_back("dirty");
  fields.push_back("size");
  fields.push_back("createTime");
  fields.push_back("lastAccessTime");
  fields.push_back("etag");
  fields.push_back("backendProtocol");
  fields.push_back("bucket_name");
  fields.push_back("obj_name");

  client.hmget(key, fields, [&key, &owner, &location, &dirty, &size, &createTime, &lastAccessTime, &etag, &backendProtocol, &bucket_name, &obj_name](cpp_redis::reply &reply){
        key = reply.as_string()[0];
        owner = reply.as_string()[1];
        location = reply.as_string()[2];
        dirty = reply.as_string()[3];
        size = reply.as_string()[4];
        createTime = reply.as_string()[5];
        lastAccessTime = reply.as_string()[6];
        etag = reply.as_string()[7];
        backendProtocol = reply.as_string()[8];
        bucket_name = reply.as_string()[9];
        obj_name = reply.as_string()[10];
  });

  obj_size = std::stoull(size);
  obj_dirty = dirty[0];

  objDir->key = key;
  objDir->owner = owner;
  objDir->location = location;
  objDir->dirty = obj_dirty;
  objDir->size = obj_size;
  objDir->createTime = createTime;
  objDir->lastAccessTime = lastAccessTime;
  objDir->etag = etag;
  objDir->backendProtocol = backendProtocol;
  objDir->bucket_name = bucket_name;
  objDir->obj_name = obj_name;

  // synchronous commit, no timeout
  client.sync_commit();

  return 0;
}

//returns all the keys between startTime and endTime as <key, time> paris
/*
std::vector<std::pair<std::string, std::string>> RGWDirectory::get_aged_keys(string startTime, string endTime){
  std::vector<std::pair<std::string, std::string>> list;
  std::string key;
  std::string time;

  cpp_redis::client client;
  //  //client.connect();
  client.connect("127.0.0.1", 7000);

  std::string dirKey = "main_directory";
  client.zrangebyscore(dirKey, startTime, endTime, true, [&key, &time](cpp_redis::reply &reply){
        for (unsigned i = 0; i < reply.as_array().size(); i+=2)
        {
            key = reply.as_string()[i];
            time = reply.as_string()[i+1];

        }
        //if (reply.is_error() == false)
  });

  // synchronous commit, no timeout
  client.sync_commit();

  return list;
}

*/


