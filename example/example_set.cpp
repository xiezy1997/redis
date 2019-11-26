/*================================================================
*   
*   File Name:    example_set.cpp
*   Creator:      Xie Zhiyong
*   Create Time:  2019.11.26
*
================================================================*/

#include "redis.h"

#include <iostream>

using namespace std;

void TestSet(RedisClient* redis) {
    string key1 = "test1";
    string member = "value1";
    string key2 = "test2";
    vector<string> members {"value1", "value2", "value3"};
    vector<string> out;
    bool ret;

    ret = redis->SAdd(key1, member);
    if (!ret) {
        cout << "SAdd Error" << endl;
    }
    else {
        cout << "SAdd OK" << endl;
    }

    ret = redis->SAdd(key2, members);
    if (!ret) {
        cout << "SAdd Error" << endl;
    }
    else {
        cout << "SAdd OK" << endl;
    }

    member = "";
    ret = redis->SPop(key2, member);
    if (!ret) {
        cout << "SPop Error" << endl;
    }
    else {
        cout << "SPop OK member: " << member << endl;
    }

    int count = 0;
    ret = redis->SCard(key2, count);
    if (!ret) {
        cout << "SCard Error" << endl;
    }
    else {
        cout << "SCard OK count: " << count << endl;
    }

    vector<string> keys {"test1", "test2"};
    out.clear();
    ret = redis->SDiff(keys, out);
    if (!ret) {
        cout << "SDiff Error" << endl;
    }
    else {
        cout << "SDiff OK" << endl;
        for (int i = 0; i < out.size(); i++) {
            if (i) cout << " ";
            cout << out[i];
        }
        cout << endl;
    }

    out.clear();
    ret = redis->SInter(keys, out);
    if (!ret) {
        cout << "SInter Error" << endl;
    }
    else {
        cout << "SInter OK" << endl;
        for (int i = 0; i < out.size(); i++) {
            if (i) cout << " ";
            cout << out[i];
        }
        cout << endl;
    }

    out.clear();
    ret = redis->SUnion(keys, out);
    if (!ret) {
        cout << "SUnion Error" << endl;
    }
    else {
        cout << "SUnion OK" << endl;
        for (int i = 0; i < out.size(); i++) {
            if (i) cout << " ";
            cout << out[i];
        }
        cout << endl;
    }

    bool is_member = "false";
    ret = redis->SIsMember(key2, "value9", is_member);
    if (!ret) {
        cout << "SIsMember Error" << endl;
    }
    else {
        cout << "SIsMember OK is_member: " << is_member << endl;
    }

    is_member = "false";
    ret = redis->SIsMember(key2, "value3", is_member);
    if (!ret) {
        cout << "SIsMember Error" << endl;
    }
    else {
        cout << "SIsMember OK is_member: " << is_member << endl;
    }

    out.clear();
    ret = redis->SMembers(key2, out);
    if (!ret) {
        cout << "SMembers Error" << endl;
    }
    else {
        cout << "SMembers OK" << endl;
        for (int i = 0; i < out.size(); i++) {
            if (i) cout << " ";
            cout << out[i];
        }
        cout << endl;
    }

    ret = redis->SRem(key1, member);
    if (!ret) {
        cout << "SRem Error" << endl;
    }
    else {
        cout << "SRem OK" << endl;
    }

    ret = redis->SRem(key2, members);
    if (!ret) {
        cout << "SRem Error" << endl;
    }
    else {
        cout << "SRem OK" << endl;
    }
}


int main(int argc, char** argv) {
    string redis_host = "127.0.0.1";
    uint32_t redis_port = 6379;
    RedisClient* redis = new RedisClient(redis_host, redis_port);
    TestSet(redis);
    delete redis;
    return 0;
}
