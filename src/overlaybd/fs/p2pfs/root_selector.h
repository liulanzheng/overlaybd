#pragma once

#include <cinttypes>
#include <string>
#include <vector>

#include "../../object.h"

namespace Cluster {
class NuwaRestful;
}

namespace Net {
struct EndPoint;
}
namespace FileSystem {

struct NodeID;
class P2PClient;

// agent select root for each file request
// so that RootSelector is able to set root list,
// and provied every Root choice for each request
class RootSelector : public Object {
public:
    // get a root id by filename
    virtual NodeID get_root(const char* filename) = 0;

    // get root next to id
    virtual NodeID get_next_root(const char* filename, NodeID root) = 0;

    // complain about failed when trying access the root
    virtual int failed_access(NodeID root) = 0;

    // clear and re-assing root list using initializer_list
    virtual int assign(std::initializer_list<NodeID> ilist) = 0;

    // clear and re-assign root list using vector
    virtual int assign(std::vector<NodeID> root_list) = 0;

    // add root node into list
    virtual int add_root(NodeID root) = 0;

    // refresh root list
    virtual int refresh() = 0;

    // clear all roots in the list
    virtual int clear() = 0;

    // timeout means when root reported as failed to access,
    // it will be freeze for timeout microseconds.
    virtual int set_timeout(uint64_t timeout) = 0;

    // get all root
    virtual std::vector<NodeID> get_root_list() = 0;

    // tell if a node is root
    virtual bool is_root(const NodeID& node) = 0;
};

// single root "selector" selector nothing but only provide root by get_root
// method.
RootSelector* new_single_root_selector(NodeID root);

}  // namespace FileSystem
