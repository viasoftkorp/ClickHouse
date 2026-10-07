#pragma once

#include <Processors/Chunk.h>
#include <Common/PODArray_fwd.h>
#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Core/SortDescription.h>
#include <Columns/IColumn.h>

namespace DB
{

using IColumnPermutation = PaddedPODArray<size_t>;

class MergeTreeReadInfo;

class IMergingAlgorithm
{
public:
    struct Status
    {
        Chunk chunk;
        bool is_finished = false;
        ssize_t required_source = -1;

        /// Additional sources that are worth reading ahead because the merge is likely
        /// to need them soon (e.g. sources deferred behind a virtual row). Unlike
        /// `required_source`, the algorithm does not wait for data from them.
        std::vector<size_t> sources_to_prefetch;

        explicit Status(Chunk chunk_) : chunk(std::move(chunk_)) {}
        explicit Status(Chunk chunk_, bool is_finished_) : chunk(std::move(chunk_)), is_finished(is_finished_) {}
        explicit Status(size_t source) : required_source(source) {}
    };

    struct Input
    {
        Chunk chunk;

        /// It is a flag which says that last row from chunk should be ignored in result.
        /// This row is not ignored in sorting and is needed to synchronize required source
        /// between different algorithm objects in parallel FINAL.
        bool skip_last_row = false;

        /// The chunk was a virtual row turned into a one-row cursor (see `prepareVirtualRowInput`):
        /// it only announces the key of the source's next rows, so besides being skipped it must
        /// not close the group of rows the algorithm is accumulating.
        bool is_virtual_row = false;

        IColumnPermutation * permutation = nullptr;

        void swap(Input & other) noexcept
        {
            chunk.swap(other.chunk);
            std::swap(skip_last_row, other.skip_last_row);
            std::swap(is_virtual_row, other.is_virtual_row);
        }

        void set(Chunk chunk_)
        {
            chunk = std::move(chunk_);
            skip_last_row = false;
            is_virtual_row = false;
        }
    };

    using Inputs = std::vector<Input>;

    static void removeConstAndSparse(Input & input)
    {
        convertToFullIfConst(input.chunk);
        convertToFullIfSparse(input.chunk);
    }

    static void removeConstAndSparse(Inputs & inputs)
    {
        for (auto & input : inputs)
            removeConstAndSparse(input);
    }

    static void removeReplicatedFromSortingColumns(const SharedHeader & header, Input & input, const SortDescription & description)
    {
        if (!input.chunk)
            return;

        size_t num_rows = input.chunk.getNumRows();
        auto columns = input.chunk.detachColumns();
        for (const auto & column_desc : description)
        {
            size_t column_number = header->getPositionByName(column_desc.column_name);
            columns[column_number] = columns[column_number]->convertToFullColumnIfReplicated();
        }
        input.chunk.setColumns(std::move(columns), num_rows);
    }

    static void removeReplicatedFromSortingColumns(const SharedHeader & header, Inputs & inputs, const SortDescription & description)
    {
        for (auto & input : inputs)
            removeReplicatedFromSortingColumns(header, input, description);
    }

    static void removeReplicatedFromSortingColumns(Input & input, const SortDescriptionWithPositions & description)
    {
        if (!input.chunk)
            return;

        size_t num_rows = input.chunk.getNumRows();
        auto columns = input.chunk.detachColumns();
        for (const auto & column_desc : description)
            columns[column_desc.column_number] = columns[column_desc.column_number]->convertToFullColumnIfReplicated();
        input.chunk.setColumns(std::move(columns), num_rows);
    }

    static void removeReplicatedFromSortingColumns(Inputs & inputs, const SortDescriptionWithPositions & description)
    {
        for (auto & input : inputs)
            removeReplicatedFromSortingColumns(input, description);
    }

    virtual const char * getName() const = 0;
    virtual void initialize(Inputs inputs) = 0;
    virtual void consume(Input & input, size_t source_num) = 0;
    virtual Status merge() = 0;

    /// A virtual row announcing the lower bound, in the merge's sort order, of every row the
    /// algorithm may still emit: the sort key of the group it is accumulating, or of the front
    /// of its queue. The merging transform passes it downstream before the next output chunk
    /// (see `IMergingTransform::work`). Null when the inputs carried no virtual rows, so there
    /// are no conversions to attach, or when the bound is not known.
    virtual std::shared_ptr<MergeTreeReadInfo> getVirtualRowForNextOutput() { return nullptr; }

    /// Called when a source the merge requested turns out to be finished without delivering
    /// any data (e.g. all its rows were filtered out upstream), so `consume` is never called
    /// for it. The merge loop already drops such a source from its queue; this notification
    /// lets algorithms that keep per-source bookkeeping (e.g. the read-ahead for sources
    /// deferred behind virtual rows) release it. The default implementation does nothing.
    virtual void onSourceExhausted(size_t /*source_num*/) {}

    IMergingAlgorithm() = default;
    virtual ~IMergingAlgorithm() = default;

    struct MergedStats
    {
        UInt64 bytes = 0;
        UInt64 rows = 0;
        UInt64 blocks = 0;
    };

    virtual MergedStats getMergedStats() const = 0;
};

// TODO: use when compile with clang which could support it
// template <class T>
// concept MergingAlgorithm = std::is_base_of<IMergingAlgorithm, T>::value;

}
