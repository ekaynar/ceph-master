


#include <cpp_redis/cpp_redis>
#include "rgw_directory.h"
#include <string>
#include <iostream>
#include <vector>
#include <list>


/* the following two functions should be implemented in their own respected classes */
int RGWDirectory::getValue(cache_obj *ptr){
	cout << "wrong function!";
}

int RGWDirectory::setValue(cache_obj *ptr){
	cout << "wrong function!";
}

/* builds the index for the directory
 * based on bucket_name, obj_name, and offset
 */
string RGWObjectDirectory::buildIndex(cache_obj *ptr){
	return ptr->bucket_name + "_" + ptr->obj_name + "_" + to_string(ptr->offset);
}

int RGWObjectDirectory::setValue(cache_obj *ptr){

	cpp_redis::client client;
	client.connect("127.0.0.1", 7000);

	//creating the index based on bucket_name, obj_name, and offset
	string key = buildIndex(ptr);

	std::vector<std::pair<std::string, std::string>> list;
	std::vector<std::string> keys;
	std::multimap<std::string, std::string> timeKey;
	std::vector<std::string> options;

	//creating a list of key's properties
	list.push_back(std::make_pair("key", key));
	list.push_back(std::make_pair("owner", ptr->user));
	list.push_back(std::make_pair("acl", ptr->acl));
	list.push_back(std::make_pair("location", ptr->location));
	list.push_back(std::make_pair("dirty", std::to_string(ptr->dirty)));
	list.push_back(std::make_pair("size", std::to_string(ptr->size_in_bytes)));
	list.push_back(std::make_pair("createTime", ptr->createTime));
	list.push_back(std::make_pair("lastAccessTime", ptr->lastAccessTime));
	list.push_back(std::make_pair("etag", ptr->etag));
	list.push_back(std::make_pair("backendProtocol", ptr->backendProtocol));
	list.push_back(std::make_pair("bucket_name", ptr->bucket_name));
	list.push_back(std::make_pair("obj_name", ptr->obj_name));

	//creating a key entry
	keys.push_back(key);

	//making key and time a pair
	timeKey.emplace(ptr->createTime,key);


	client.hmset(key, list, [](cpp_redis::reply &reply){
	});

	client.rpush("objectDirectory", keys, [](cpp_redis::reply &reply){
	});

	//this will be used for aging policy
	client.zadd("keyDirectory", options, timeKey, [](cpp_redis::reply &reply){
	});

	// synchronous commit, no timeout
	client.sync_commit();
	return 0;

}


int RGWObjectDirectory::getValue(cache_obj *ptr){

    string key = buildIndex(ptr);
    string owner;
    string acl;
    string location;
    string dirty;
    string size;
    string createTime;
    string lastAccessTime;
    string etag;
	string offset;
    string backendProtocol;
    string bucket_name;
    string obj_name;

	cpp_redis::client client;
	client.connect("127.0.0.1", 7000);

	std::vector<std::string> fields;
	fields.push_back("key");
	fields.push_back("owner");
	fields.push_back("acl");
	fields.push_back("location");
	fields.push_back("dirty");
	fields.push_back("size");
	fields.push_back("createTime");
	fields.push_back("lastAccessTime");
	fields.push_back("etag");
	fields.push_back("backendProtocol");
	fields.push_back("bucket_name");
	fields.push_back("obj_name");

	client.hmget(key, fields, [&key, &owner, &acl, &location, &dirty, &size, &createTime, &lastAccessTime, &etag, &backendProtocol, &bucket_name, &obj_name](cpp_redis::reply &reply){
	      key = reply.as_string()[0];
	      owner = reply.as_string()[1];
	      acl = reply.as_string()[2];
	      location = reply.as_string()[3];
	      dirty = reply.as_string()[4];
	      size = reply.as_string()[5];
	      createTime = reply.as_string()[6];
	      lastAccessTime = reply.as_string()[7];
	      etag = reply.as_string()[8];
  		  backendProtocol = reply.as_string()[9];
  		  bucket_name = reply.as_string()[10];
	      obj_name = reply.as_string()[11];
	});

	//finding offest -> key = bucketname_objName_offset
	size_t pos = key.rfind("_");
	offset = key.substr(pos++);

	ptr->user = owner;
	ptr->acl = acl;
	ptr->location = location;
	ptr->dirty = dirty[0];
	ptr->size_in_bytes = stoull(size);
	ptr->createTime = createTime;
	ptr->lastAccessTime = lastAccessTime;
	ptr->etag = etag;
	ptr->offset = stoull(offset);
	ptr->backendProtocol = backendProtocol;
	ptr->bucket_name = bucket_name;
	ptr->obj_name = obj_name;

	// synchronous commit, no timeout
	client.sync_commit();

	return 0;
}

/* updatinh the directory value*/
/* value should be string even for fields such as size or offset */
int RGWObjectDirectory::updateValue(cache_obj *ptr, string field, string value){

	cache_obj tmpObj;
	
	//we need to build the key to find the object in the directory
	tmpObj.bucket_name = ptr->bucket_name;
	tmpObj.obj_name = ptr->obj_name;
	tmpObj.offset = ptr->offset;

	//getting old values from the directory
	getValue(&tmpObj);

	//updating the desired field
	if (field == "user")
		tmpObj.user = value;	
	else if (field == "bucket_name")
		tmpObj.bucket_name = value;
	else if (field == "obj_name")
		tmpObj.obj_name = value;
	else if (field == "location")
		tmpObj.location = value;
	else if (field == "size_in_bytes")
		tmpObj.size_in_bytes = stoull(value);
	else if (field == "dirty")
		tmpObj.dirty = value[0];
	else if (field == "offset")
		tmpObj.offset = stoull(value);
	else if (field == "etag")
		tmpObj.etag = value;
	else if (field == "createTime")
		tmpObj.createTime = value;
	else if (field == "lastAccessTime")
		tmpObj.lastAccessTime = value;
	else if (field == "backendProtocol")
		tmpObj.backendProtocol = value;
	else if (field == "acl")
		tmpObj.acl = value;

	//updating the directory value 
	setValue(&tmpObj);
}


int RGWObjectDirectory::delValue(cache_obj *ptr){
    string key = buildIndex(ptr);
	int result = 0;

	result += delKey(key);
	return result;
}

int RGWObjectDirectory::delKey(string key){
	int result = 0;
    std::vector<std::string> keys;
    keys.push_back(key);

	cpp_redis::client client;
	client.connect("127.0.0.1", 7000);

	client.del(keys, [&result](cpp_redis::reply &reply){
		result = reply.as_integer();
	});
	return result;
}


//returns all the keys between startTime and endTime as <key, time> paris
/*
std::vector<std::pair<std::string, std::string>> RGWDirectory::get_aged_keys(string startTime, string endTime){
	std::vector<std::pair<std::string, std::string>> list;
	std::string key;
	std::string time;

	cpp_redis::client client;
	//	//client.connect();
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


