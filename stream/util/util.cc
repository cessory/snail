#include "util.h"
namespace snail {

seastar::temporary_buffer<char> foreign_buffer_copy(
    seastar::foreign_ptr<std::unique_ptr<seastar::temporary_buffer<char>>>
        org) {
    if (org.get_owner_shard() == seastar::this_shard_id()) {
        return std::move(*org);
    }
    seastar::temporary_buffer<char> *one = org.get();
    return seastar::temporary_buffer<char>(one->get_write(), one->size(),
                                           make_object_deleter(std::move(org)));
}

}  // namespace snail
