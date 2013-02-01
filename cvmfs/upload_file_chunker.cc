#include "upload_file_chunker.h"

#include <algorithm>

using namespace upload;

ChunkGenerator::ChunkGenerator(const MemoryMappedFile &mmf) :
  mmf_(mmf),
  offset_(0)
{
  assert (mmf.IsMapped());
}

ChunkGenerator::~ChunkGenerator() {}

Chunk ChunkGenerator::Next() {
  assert (HasMoreData());
  const off_t  next_cut_mark = FindNextCutMark();
  const off_t  chunk_offset = offset_;
  const size_t chunk_size   = next_cut_mark - chunk_offset;

  offset_ = next_cut_mark;
  return Chunk(chunk_offset, chunk_size);
}


off_t ChunkGenerator::FindNextCutMark() const {
  assert (HasMoreData());

  return offset_ + std::min(8ul * 1024ul * 1024ul, mmf_.size() - offset_);
}

bool ChunkGenerator::HasMoreData() const {
  return offset_ < mmf_.size();
}

