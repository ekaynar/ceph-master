


#include <cpp_redis/cpp_redis>
#include "rgw_directory.h"
#include <string>
#include <iostream>
#include <vector>
#include <list>


int RGWDirectory::setMetaValue(string key, string timeStr, string bucket_name, string obj_name, string location, string owner, uint64_t obj_size, string etag){

  cpp_redis::client client;
  //  //client.connect();
  client.connect("127.0.0.1", 7000);

  //dout(10) << __func__ << " redis set key ="  << key << dendl;
  std::vector<std::pair<std::string, std::string>> list;
  std::vector<std::string> keys;
  std::multimap<std::string, std::string> timeKey;
  std::vector<std::string> options;

  //creating a list of key's properties
  list.push_back(std::make_pair("key", key));
  list.push_back(std::make_pair("owner", owner));
  list.push_back(std::make_pair("time", timeStr));
  list.push_back(std::make_pair("bucket_name", bucket_name));
  list.push_back(std::make_pair("obj_name", obj_name));
  list.push_back(std::make_pair("location", location));
  list.push_back(std::make_pair("size", std::to_string(obj_size)));
  list.push_back(std::make_pair("etag", etag));

  //creating a key entry
  keys.push_back(key);

  //making key and time a pair
  timeKey.emplace(timeStr,key);


  client.hmset(key, list, [](cpp_redis::reply &reply){
  });

  client.rpush("directory", keys, [](cpp_redis::reply &reply){
  });


  client.zadd("main_directory", options, timeKey, [](cpp_redis::reply &reply){
  });

  // synchronous commit, no timeout
  client.sync_commit();
  return 0;

}


int RGWDirectory::getMetaValue(directory_values &dir_val){

  string key;
  string owner;
  string time;
  string bucket_name;
  string obj_name;
  string location;
  string etag;
  string sizeStr;
  uint64_t obj_size;


  cpp_redis::client client;
  //  //client.connect();
  client.connect("127.0.0.1", 7000);

  key = dir_val.key;

  std::vector<std::string> fields;
  fields.push_back("key");
  fields.push_back("owner");
  fields.push_back("time");
  fields.push_back("bucket_name");
  fields.push_back("obj_name");
  fields.push_back("location");
  fields.push_back("size");
  fields.push_back("etag");

  client.hmget(key, fields, [&key, &owner, &time, &bucket_name, &obj_name, &location, &etag, &sizeStr](cpp_redis::reply &reply){
        //std::cout << reply << '\n';
        key = reply.as_string()[0];
        owner = reply.as_string()[1];
        time = reply.as_string()[2];
        bucket_name = reply.as_string()[3];
        obj_name = reply.as_string()[4];
        location = reply.as_string()[5];
        sizeStr = reply.as_string()[6];
        etag = reply.as_string()[7];
  });

  //obj_size = std::strtoull(sizeStr.c_str(), NULL, 0);
  obj_size = std::stoull(sizeStr);

  dir_val.key = key;
  dir_val.owner = owner;
  dir_val.time = time;
  dir_val.bucket_name = bucket_name;
  dir_val.obj_name = obj_name;
  dir_val.location = location;
  dir_val.obj_size = obj_size;
  dir_val.etag = etag;

  // synchronous commit, no timeout
  client.sync_commit();

  return 0;
}

//returns all the keys between startTime and endTime as <key, time> paris
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




