
#include "image_service.h"
#include "overlaybd/alog.h"
#include "overlaybd/fs/filesystem.h"
#include "image_file.h"
#include "overlaybd/event-loop.h"
#include "overlaybd/photon/thread-pool.h"
#include "overlaybd/photon/thread.h"
#include "overlaybd/photon/syncio/fd-events.h"
#include <scsi/scsi.h>
#include <sys/resource.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <malloc.h>
#include <errno.h>
#include "libtcmu.h"
#include "libtcmu_common.h"
#include "libtcmu_priv.h"
#include "scsi.h"
#include "scsi_defs.h"
#include "scsi_helper.h"
#include "target_core_user_local.h"
#include "obd_device.h"

#define ALIGNMENT 4096

photon::ThreadPool<32> threadpool;

struct handle_args {
    struct ObdDevice *dev;
    struct tcmulib_cmd *cmd;
};

void *thread_pool_cmd_handle(void *args) {
    handle_args *obj = (handle_args *)args;
    obj->dev->cmd_handler(obj->cmd);
    delete obj;
    return nullptr;
}

ObdDevice::ObdDevice(struct tcmu_device *tcmu_dev, ImageFile *file, uint32_t recycle_sec)
    : tcmu_dev(tcmu_dev), file(file), recycle_sec(recycle_sec) {
    fd = tcmu_dev_get_fd(tcmu_dev);
    loop = new_event_loop({this, &ObdDevice::loop_wait_for_readable},
                          {this, &ObdDevice::loop_on_accept});
    loop->async_run();
    // mem_reset_jh =
    //     photon::thread_enable_join(photon::thread_create11(&ObdDevice::mem_reset_thread, this));
}

ObdDevice::~ObdDevice() {
    delete loop;
    file->close();
    delete file;
    file = nullptr;
    // photon::thread_shutdown((photon::thread *)mem_reset_jh);
    // photon::thread_join(mem_reset_jh);
}

void ObdDevice::cmd_handler(struct tcmulib_cmd *cmd) {
    size_t ret = -1;
    size_t length;

    switch (cmd->cdb[0]) {
    case INQUIRY:
        photon::thread_yield();
        ret = tcmu_emulate_inquiry(tcmu_dev, NULL, cmd->cdb, cmd->iovec, cmd->iov_cnt);
        tcmulib_command_complete(tcmu_dev, cmd, ret);
        break;

    case TEST_UNIT_READY:
        photon::thread_yield();
        ret = tcmu_emulate_test_unit_ready(cmd->cdb, cmd->iovec, cmd->iov_cnt);
        tcmulib_command_complete(tcmu_dev, cmd, ret);
        break;

    case SERVICE_ACTION_IN_16:
        photon::thread_yield();
        if (cmd->cdb[1] == READ_CAPACITY_16)
            ret = tcmu_emulate_read_capacity_16(file->num_lbas, file->block_size, cmd->cdb,
                                                cmd->iovec, cmd->iov_cnt);
        else
            ret = TCMU_STS_NOT_HANDLED;
        tcmulib_command_complete(tcmu_dev, cmd, ret);
        break;

    case MODE_SENSE:
    case MODE_SENSE_10:
        photon::thread_yield();
        ret = emulate_mode_sense(tcmu_dev, cmd->cdb, cmd->iovec, cmd->iov_cnt, file->read_only);
        tcmulib_command_complete(tcmu_dev, cmd, ret);
        break;

    case MODE_SELECT:
    case MODE_SELECT_10:
        photon::thread_yield();
        ret = tcmu_emulate_mode_select(tcmu_dev, cmd->cdb, cmd->iovec, cmd->iov_cnt);
        tcmulib_command_complete(tcmu_dev, cmd, ret);
        break;

    case READ_6:
    case READ_10:
    case READ_12:
    case READ_16:
        length = tcmu_iovec_length(cmd->iovec, cmd->iov_cnt);
        ret = file->preadv(cmd->iovec, cmd->iov_cnt, tcmu_cdb_to_byte(tcmu_dev, cmd->cdb));
        if (ret == length) {
            tcmulib_command_complete(tcmu_dev, cmd, TCMU_STS_OK);
        } else {
            tcmulib_command_complete(tcmu_dev, cmd, TCMU_STS_RD_ERR);
        }
        break;

    case WRITE_6:
    case WRITE_10:
    case WRITE_12:
    case WRITE_16:
        length = tcmu_iovec_length(cmd->iovec, cmd->iov_cnt);
        ret = file->pwritev(cmd->iovec, cmd->iov_cnt, tcmu_cdb_to_byte(tcmu_dev, cmd->cdb));
        if (ret == length) {
            tcmulib_command_complete(tcmu_dev, cmd, TCMU_STS_OK);
        } else {
            if (errno == EROFS) {
                tcmulib_command_complete(tcmu_dev, cmd, TCMU_STS_WR_ERR_INCOMPAT_FRMT);
            }
            tcmulib_command_complete(tcmu_dev, cmd, TCMU_STS_WR_ERR);
        }
        break;

    case SYNCHRONIZE_CACHE:
    case SYNCHRONIZE_CACHE_16:
        ret = file->fdatasync();
        if (ret == 0) {
            tcmulib_command_complete(tcmu_dev, cmd, TCMU_STS_OK);
        } else {
            tcmulib_command_complete(tcmu_dev, cmd, TCMU_STS_WR_ERR);
        }
        break;

    case WRITE_SAME:
    case WRITE_SAME_16:
        if (cmd->cdb[1] & 0x08) {
            length = tcmu_iovec_length(cmd->iovec, cmd->iov_cnt);
            ret = file->fallocate(3, tcmu_cdb_to_byte(tcmu_dev, cmd->cdb), length);
            if (ret == 0) {
                tcmulib_command_complete(tcmu_dev, cmd, TCMU_STS_OK);
            } else {
                tcmulib_command_complete(tcmu_dev, cmd, TCMU_STS_WR_ERR);
            }
        } else {
            LOG_ERROR("unknown write_same command `", cmd->cdb[0]);
            tcmulib_command_complete(tcmu_dev, cmd, TCMU_STS_NOT_HANDLED);
        }
        break;

    case MAINTENANCE_IN:
    case MAINTENANCE_OUT:
        tcmulib_command_complete(tcmu_dev, cmd, TCMU_STS_NOT_HANDLED);
        break;

    default:
        LOG_ERROR("unknown command `", cmd->cdb[0]);
        tcmulib_command_complete(tcmu_dev, cmd, TCMU_STS_NOT_HANDLED);
        break;
    }

    // call tcmulib_processing_complete(dev) if needed
    ++aio_pending_wakeups;
    int wake_up = (aio_pending_wakeups == 1) ? 1 : 0;
    while (wake_up) {
        tcmulib_processing_complete(tcmu_dev);
        photon::thread_yield();

        if (aio_pending_wakeups > 1) {
            aio_pending_wakeups = 1;
            wake_up = 1;
        } else {
            aio_pending_wakeups = 0;
            wake_up = 0;
        }
    }
    last_io_time = photon::now;
}

int ObdDevice::loop_wait_for_readable(EventLoop *) {
    auto ret = photon::wait_for_fd_readable(fd);
    if (ret < 0) {
        if (errno == ETIMEDOUT) {
            return 0;
        }
        return -1;
    }
    return 1;
}

int ObdDevice::loop_on_accept(EventLoop *) {
    struct tcmulib_cmd *cmd;
    tcmulib_processing_start(tcmu_dev);
    while ((cmd = tcmulib_get_next_command(tcmu_dev, 0)) != NULL) {
        threadpool.thread_create(&thread_pool_cmd_handle, new handle_args{this, cmd});
        // photon::thread_create(&thread_pool_cmd_handle, new handle_args{this, cmd});
    }
    return 0;
}

void ObdDevice::reset_buffer() {
    if (tcmu_cfgfs_dev_exec_action(tcmu_dev, "block_dev", 1) == 0) {
        struct tcmu_mailbox *mb = tcmu_dev->map;
        while (mb->cmd_head != mb->cmd_tail) {
            LOG_DEBUG("waiting for ring to clear");
            photon::thread_usleep(20 * 1000);
        }

        size_t offset = (mb->cmdr_off + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;
        int res = ::madvise((char *)mb + offset, tcmu_dev->map_len - offset, MADV_DONTNEED);
        LOG_DEBUG("madvise result `", res);
        tcmu_cfgfs_dev_exec_action(tcmu_dev, "block_dev", 0);
    }
}

void ObdDevice::mem_reset_thread() {
    while (true) {
        photon::thread_sleep(recycle_sec);
        if (file == nullptr)
            break; // exit
        if (last_io_time > 0 && photon::now - last_io_time > recycle_sec * 1000 * 1000) {
            reset_buffer();
            last_io_time = 0;
        }
    }
    LOG_DEBUG("mem_reset_thread exit");
}