#include "root_selector.h"

#include <map>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>
#include <set>
#include <thread>
#include <sys/fcntl.h>
#include <libgen.h>

#include "../../estring.h"
#include "../../event-loop.h"
#include "../../photon/syncio/fd-events.h"
#include "../../photon/thread.h"
#include "../../photon/timer.h"
#include "../../timeout.h"
#include "../../utility.h"
#include "client.h"
#include "protocol.h"
#include "../checkedfs/tool/crc32c.h"
#include "../../expirecontainer.h"

namespace FileSystem {

#define UNIMPLEMENTED(func) \
    virtual func {          \
        errno = ENOSYS;     \
        return -1;          \
    }
class SingleRootSelector : public RootSelector {
protected:
    NodeID m_root;

public:
    // initial using endpoint, generate random id for root
    explicit SingleRootSelector(NodeID root) : m_root(root) {}

    UNIMPLEMENTED(int assign(std::initializer_list<NodeID>) override);

    UNIMPLEMENTED(int assign(std::vector<NodeID>) override);

    UNIMPLEMENTED(int refresh() override);

    std::vector<NodeID> get_root_list() override { return {m_root}; }

    virtual NodeID get_next_root(const char*, NodeID) override {
        return m_root;
    }

    // return the only root node
    virtual NodeID get_root(const char*) override { return m_root; }

    // single root had no choice but retry same root
    virtual int failed_access(NodeID) override { return 0; }

    virtual int add_root(NodeID root) override {
        m_root = root;
        return 0;
    }

    // clear just
    virtual int clear() override {
        m_root = NodeID();
        return 0;
    }

    // timeout is meanless for single root
    virtual int set_timeout(uint64_t timeout) override { return 0; }

    virtual bool is_root(const NodeID& node) override {
        return m_root.endpoint() == node.endpoint();
    }
};


RootSelector* new_single_root_selector(NodeID id) {
    return new SingleRootSelector(id);
}

}  // namespace FileSystem
