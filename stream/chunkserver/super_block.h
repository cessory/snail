#pragma once

namespace snail {
namespace stream {

enum {
    EXTENT_PT = 0,
    CHUNK_PT = 1,
    LOGA_PT = 2,
    LOGB_PT = 3,
    DATA_PT = 4,
    MAX_PT = 5,
};

struct Partition {
    uint64_t start = 0;
    uint64_t size = 0;
};

struct SuperBlock {
    uint32_t magic = 0;
    uint32_t version = 0;
    uint32_t cluster_id = 0;
    DevType dev_type = DevType::HDD;
    uint32_t dev_id = 0;
    uint32_t sector_size = 512;
    uint64_t capacity = 0;
    Partition pt[MAX_PT];  // extent meta pt[0] chunk meta pt[1]
                           // log pt[2] pt[3] data pt[4]

    void MarshalTo(char *b);

    void Unmarshal(const char *b, size_t len);

    inline uint64_t ExtentMetaOffset() const { return pt[EXTENT_PT].start; }
    inline uint64_t ExtentMetaSize() const { return pt[EXTENT_PT].size; }

    inline uint64_t ChunkMetaOffset() const { return pt[CHUNK_PT].start; }
    inline uint64_t ChunkMetaSize() const { return pt[CHUNK_PT].size; }

    inline uint64_t Log1Offset() const { return pt[LOGA_PT].start; }
    inline uint64_t Log1Size() const { return pt[LOGA_PT].size; }
    inline uint64_t Log2Offset() const { return pt[LOGB_PT].start; }
    inline uint64_t Log2Size() const { return pt[LOGB_PT].size; }

    inline uint64_t DataOffset() const { return pt[DATA_PT].start; }
    inline uint64_t DataSize() const { return pt[DATA_PT].size; }
};

}  // namespace stream
}  // namespace snail
