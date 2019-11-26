/*================================================================
*   
*   File Name:    example_hash.cpp
*   Creator:      Xie Zhiyong
*   Create Time:  2019.11.26
*
================================================================*/

#include "redis.h"

#include <iostream>

using namespace std;

void TestHash(RedisClient* redis) {
    bool ret;
    string key = "test";
    string field = "key1";
    string value = "value1";

    ret = redis->HSet(key, field, value);
    if (!ret) {
        cout << "HSet Error" << endl;
    }
    else {
        cout << "HSet OK" << endl;
    }

    value = "";
    ret = redis->HGet(key, field, value);
    if (!ret) {
        cout << "HGet Error" << endl;
    }
    else {
        cout << "HGet value: " << value << endl;
    }

    bool is_exists = false;
    ret = redis->HExists(key, field, is_exists);
    if (!ret) {
        cout << "HExists Error" << endl;
    }
    else {
        cout << "HExists OK is_exists: " << is_exists << endl;
    }

    ret = redis->HDel(key, field);
    if (!ret) {
        cout << "HDel Error" << endl;
    }
    else {
        cout << "HDel OK" << endl;
    }

    is_exists = false;
    ret = redis->HExists(key, field, is_exists);
    if (!ret) {
        cout << "HExists Error" << endl;
    }
    else {
        cout << "HExists OK is_exists: " << is_exists << endl;
    }
    
    vector<string> fields {"key2", "key3", "key4", "key5"};
    vector<string> values {"value2", "value3", "11", "1.2"};
    vector<string> out;
    ret = redis->HMSet(key, fields, values);
    if (!ret) {
        cout << "HMSet Error" << endl;
    }
    else {
        cout << "HMSet OK" << endl;
    }

    ret = redis->HMGet(key, fields, out);
    if (!ret) {
        cout << "HMGet Error" << endl;
    }
    else {
        cout << "HMGet OK" << endl;
        for (int i = 0; i < out.size(); i++) {
            cout << fields[i] << " : " << values[i] << " : " << out[i] << endl;
        }
    }

    int length = 0;
    ret = redis->HLen(key, length);
    if (!ret) {
        cout << "HLen Error" << endl;
    }
    else {
        cout << "HLen OK length: " << length<< endl;
    }

    out.clear();
    ret = redis->HKeys(key, out);
    if (!ret) {
        cout << "HKeys Error" << endl;
    }
    else {
        cout << "HKeys OK" << endl;
        for (int i = 0; i < out.size(); i++) {
            {

            }cout << fields[i] << " : " << out[i] << endl;
        }
    }

    out.clear();
    ret = redis->HVals(key, out);
    if (!ret) {
        cout << "HVals Error" << endl;
    }
    else {
        cout << "HVals OK" << endl;
        for  (int i = 0; i < out.size(); i++) {
            cout << values[i] << " : " << out[i] << endl;
        }
    }

    out.clear();
    vector<string> out2;
    ret = redis->HGetAll(key, out, out2);
    if (!ret) {
        cout << "HGetAll Error" << endl;
    }
    else {
        cout << "HGetAll OK" << endl;
        for (int i = 0; i < out.size(); i++) {
            cout << fields[i] << " : " << out[i] << " : " << values[i] << " : " << out2[i] << endl;
        }
    }

    ret = redis->HIncrBy(key, fields[2], 2);
    if (!ret) {
        cout << "HIncrBy Error" << endl;
    }
    else {
        cout << "HIncrBy OK" << endl;
    }
    
    ret = redis->HIncrByFloat(key, fields[3], 2.5);
    if (!ret) {
        cout << "HIncrByFloat Error" << endl;
    }
    else {
        cout << "HIncrByFloat OK" << endl;
    }
    
    out.clear();
    ret = redis->HMGet(key, fields, out);
    if (!ret) {
        cout << "HMGet Error" << endl;
    }
    else {
        cout << "HMGet OK" << endl;
        for (int i = 0; i < out.size(); i++) {
            cout << fields[i] << " : " << values[i] << " : " << out[i] << endl;
        }
    }

    ret = redis->HDel(key, fields);
    if (!ret) {
        cout << "HDel Error" << endl;
    }
    else {
        cout << "HDel OK" << endl;
    }

    out.clear();
    ret = redis->HMGet(key, fields, out);
    if (!ret) {
        cout << "HMGet Error" << endl;
    }
    else {
        cout << "HMGet OK" << endl;
        for (int i = 0; i < out.size(); i++) {
            cout << fields[i] << " : " << values[i] << " : " << out[i] << endl;
        }
    }
    
    ret = redis->Del(key);
    if (!ret) {
        cout << "Del Error" << endl;
    }
    else {
        cout << "Del OK" << endl;
    }
}

int main(int argc, char** argv) {
    string redis_host = "127.0.0.1";
    uint32_t redis_port = 6379;
    RedisClient* redis = new RedisClient(redis_host, redis_port);
    TestHash(redis);
    delete redis;
    return 0;
}
