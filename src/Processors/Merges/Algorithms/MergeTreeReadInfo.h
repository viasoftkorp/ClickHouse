#pragma once

#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/// To carry part level and virtual row if chunk is produced by a merge tree source
class MergeTreeReadInfo : public ChunkInfoCloneable<MergeTreeReadInfo>
{
public:
    MergeTreeReadInfo() = default;
    explicit MergeTreeReadInfo(size_t part_level);
    explicit MergeTreeReadInfo(size_t part_level, const Block & pk_block_, ExpressionActionsPtr virtual_row_conversions_);
    MergeTreeReadInfo(const MergeTreeReadInfo & other);

    size_t origin_merge_tree_part_level = 0;

    /// If is virtual_row, block should not be empty.
    Block pk_block;
    ExpressionActionsPtr virtual_row_conversions;
};

size_t getPartLevelFromChunk(const Chunk & chunk);

bool isVirtualRow(const Chunk & chunk);

/// Returns the block the virtual row was built from, i.e. the sort columns it can announce exactly.
/// Every other column of `header` is filled with a type default and bounds nothing.
Block setVirtualRow(Chunk & chunk, const Block & header, bool apply_virtual_row_conversions);

/// For a merging algorithm whose inputs may start a block with a virtual row: turns the virtual row
/// into a one-row cursor chunk that is skipped (`skip_last_row`) and never emitted, so that the
/// source is positioned at the announced key without reading its data (see `MergingSortedAlgorithm`).
/// The conversions carried by the virtual row are kept in `conversions` for the virtual rows the
/// algorithm announces itself. Returns false and leaves the input untouched if it is not a virtual row.
bool prepareVirtualRowInput(IMergingAlgorithm::Input & input, const Block & header, ExpressionActionsPtr & conversions);

/// A virtual row announcing that every row a merge still emits is at or after row `row` of
/// `sort_columns` (the columns of `description`, in that order) in the merge's sort order.
/// The columns are stored under their names in `header`, so a merge downstream over the same
/// columns, or over the expressions `conversions` compute from them, can position the stream by it.
std::shared_ptr<MergeTreeReadInfo> makeVirtualRowInfo(
    const Block & header,
    const SortDescription & description,
    const IColumn * const * sort_columns,
    size_t row,
    ExpressionActionsPtr conversions);

}
