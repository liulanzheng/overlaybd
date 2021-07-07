#pragma once

#include "image_file.h"
#include "overlaybd/event-loop.h"
#include "libtcmu.h"
#include "overlaybd/photon/thread-pool.h"

class ObdDevice {
public:
    ObdDevice(struct tcmu_device *tcmu_dev, ImageFile *file);
    ~ObdDevice();

    ImageFile *file = nullptr;
    uint32_t aio_pending_wakeups = 0;
    struct tcmu_device *tcmu_dev;

    void cmd_handler(struct tcmulib_cmd *cmd);

private:
    photon::join_handle *mem_reset_jh = nullptr;
    int fd;
    photon::ThreadPool<32> threadpool;
    uint64_t last_io_time = 0;
    EventLoop *loop;

    int loop_wait_for_readable(EventLoop *);
    int loop_on_accept(EventLoop *);
    void reset_buffer();
    void mem_reset_thread();
};
