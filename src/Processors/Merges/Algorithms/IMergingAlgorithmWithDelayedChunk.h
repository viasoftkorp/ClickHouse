#pragma once

#include <Core/Block_fwd.h>
#include <Core/SortCursor.h>
#include <Core/SortDescription.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/Merges/Algorithms/RowRef.h>

namespace DB
{

class IMergingAlgorithmWithDelayedChunk : public IMergingAlgorithm
{
public:
    IMergingAlgorithmWithDelayedChunk(SharedHeader header_, size_t num_inputs, SortDescription description_);

    size_t prev_unequal_column = 0;

protected:
    /// The batch queue identifies how many consecutive rows can be taken from the front
    /// cursor in one go (see `SortingQueueImpl::updateBatchSize`), so consuming rows one by
    /// one with `next(1)` restructures the queue once per batch instead of once per row.
    SortingQueueForCursor<SortCursor, SortingQueueStrategy::Batch> queue;
    SortDescription description;

    /// Previous row. May refer to last_chunk_sort_columns or row from source_chunks.
    detail::RowRef last_key;

    ColumnRawPtrs last_chunk_sort_columns; /// Point to last_chunk if valid.

    /// From the first virtual row seen at the inputs (the derived algorithm converts them with
    /// `prepareVirtualRowInput` before preprocessing its chunks); null while none was seen.
    ExpressionActionsPtr virtual_row_conversions;

    void initializeQueue(Inputs inputs);
    void updateCursor(Input & input, size_t source_num);
    bool skipLastRowFor(size_t input_number) const { return current_inputs[input_number].skip_last_row; }
    bool isVirtualRowInput(size_t input_number) const { return current_inputs[input_number].is_virtual_row; }

    /// `getVirtualRowForNextOutput` for the bound at `row` of `sort_columns` (the key of the group
    /// being accumulated, or the front of the queue). Null when the inputs carried no virtual rows.
    std::shared_ptr<MergeTreeReadInfo> makeVirtualRowBound(const IColumn * const * sort_columns, size_t row) const
    {
        if (!virtual_row_conversions)
            return nullptr;
        return makeVirtualRowInfo(*header, description, sort_columns, row, virtual_row_conversions);
    }

    /// The bound at the front of the queue, for an algorithm that is not accumulating a group.
    std::shared_ptr<MergeTreeReadInfo> makeVirtualRowBoundFromQueue()
    {
        if (!queue.isValid())
            return nullptr;
        const auto & current = *queue.current().first;
        return makeVirtualRowBound(current->sort_columns.data(), current->getRow());
    }

    void setRowRef(detail::RowRef & row, SortCursor & cursor) { row.set(cursor); }
    bool rowsHaveDifferentSortColumns(const detail::RowRef & lhs, const detail::RowRef & rhs)
    {
        /// By the time this method is called, `inputs_origin_merge_tree_part_level[lhs.source_stream_index]` must have been
        /// initialized in either `initializeQueue` or `updateCursor`
        if (lhs.source_stream_index == rhs.source_stream_index && inputs_origin_merge_tree_part_level[lhs.source_stream_index] > 0)
            return true;

        auto first_non_equal = lhs.firstNonEqualSortColumnsWith(prev_unequal_column, rhs);

        if (first_non_equal < lhs.num_columns)
        {
            prev_unequal_column = first_non_equal;
            return true;
        }

        return false;
    }

    SharedHeader header;

private:
    /// Inputs currently being merged.
    Inputs current_inputs;
    SortCursorImpls cursors;

    std::vector<size_t> inputs_origin_merge_tree_part_level;

    /// In merging algorithm, we need to compare current sort key with the last one.
    /// So, sorting columns for last row needed to be stored.
    /// In order to do it, we extend lifetime of last chunk and it's sort columns (from corresponding sort cursor).
    Chunk last_chunk;
};

}
