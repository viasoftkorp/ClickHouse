#pragma once

#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>

#include <Core/Block_fwd.h>
#include <Processors/IProcessor.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

namespace DB
{

/// Base class for IMergingTransform.
/// It is needed to extract all non-template methods in single translation unit.
class IMergingTransformBase : public IProcessor
{
public:
    IMergingTransformBase(
        size_t num_inputs,
        SharedHeader & input_header,
        SharedHeader & output_header,
        bool have_all_inputs_,
        UInt64 limit_hint_,
        bool always_read_till_end_);

    IMergingTransformBase(
        SharedHeaders & input_headers,
        SharedHeader & output_header,
        bool have_all_inputs_,
        UInt64 limit_hint_,
        bool always_read_till_end_);

    OutputPort & getOutputPort();

    /// Adds an input before registration is closed with `setHaveAllInputs`. Its header may differ
    /// from the output header when the algorithm changes the column layout.
    void addInput(const Block & input_header);
    /// Closes input registration when constructed with `have_all_inputs_` set to false.
    void setHaveAllInputs();

    Status prepare() override;

    /// Announce the lower bound of the rows still to come as a virtual row before the first
    /// output chunk and, with `per_block`, after every one (see `IMergingAlgorithm::getVirtualRowForNextOutput`).
    /// For a merge whose output is merged again downstream: the FINAL merges of a read in order.
    void enableVirtualRowAnnouncement(bool per_block)
    {
        announce_virtual_rows = true;
        announce_virtual_row_per_block = per_block;
    }

protected:
    virtual void onNewInput(); /// Is called when new input is added. Only if have_all_inputs = false.
    virtual void onFinish() {} /// Is called when all data is processed.

    /// Processor state.
    struct State
    {
        Chunk output_chunk;
        IMergingAlgorithm::Input input_chunk;
        bool has_input = false;
        bool is_finished = false;
        bool need_data = false;
        bool no_data = false;
        size_t next_input_to_read = 0;

        /// Inputs to ask for data without waiting for it (read-ahead for sources
        /// deferred behind virtual rows). See `IMergingAlgorithm::Status::sources_to_prefetch`.
        std::vector<size_t> inputs_to_prefetch;

        IMergingAlgorithm::Inputs init_chunks;
    };

    State state;

    bool announce_virtual_rows = false;
    bool announce_virtual_row_per_block = false;
    /// An output chunk went out and the bound for the rows after it is still to be announced.
    bool virtual_row_announcement_pending = false;

private:
    struct InputState
    {
        explicit InputState(InputPort & port_) : port(port_) {}

        InputPort & port;
        bool is_initialized = false;
    };

    std::vector<InputState> input_states;
    std::atomic<bool> have_all_inputs;
    bool is_initialized = false;
    UInt64 limit_hint = 0;
    bool always_read_till_end = false;

    IProcessor::Status prepareInitializeInputs();
};

/// Implementation of MergingTransform using IMergingAlgorithm.
template <typename Algorithm>
class IMergingTransform : public IMergingTransformBase
{
public:
    template <typename ... Args>
    IMergingTransform(
        size_t num_inputs,
        SharedHeader input_header,
        SharedHeader output_header,
        bool have_all_inputs_,
        UInt64 limit_hint_,
        bool always_read_till_end_,
        Args && ... args)
        : IMergingTransformBase(num_inputs, input_header, output_header, have_all_inputs_, limit_hint_, always_read_till_end_)
        , algorithm(std::forward<Args>(args) ...)
    {
    }

    template <typename ... Args>
    IMergingTransform(
        SharedHeaders input_headers,
        SharedHeader output_header,
        bool have_all_inputs_,
        UInt64 limit_hint_,
        bool always_read_till_end_,
        bool empty_chunk_on_finish_,
        Args && ... args)
        : IMergingTransformBase(input_headers, output_header, have_all_inputs_, limit_hint_, always_read_till_end_)
        , empty_chunk_on_finish(empty_chunk_on_finish_)
        , algorithm(std::forward<Args>(args) ...)
    {
    }

    void work() override
    {
        Stopwatch watch{CLOCK_MONOTONIC_COARSE};

        if (!state.init_chunks.empty())
        {
            algorithm.initialize(std::move(state.init_chunks));

            /// Announced before the merge produces anything, so that a merge downstream can
            /// defer this stream, and with it the inputs it has not pulled from yet.
            if (announce_virtual_rows && announceVirtualRow())
                return;
        }

        if (state.has_input)
        {
            // std::cerr << "Consume chunk with " << state.input_chunk.getNumRows()
            //           << " for input " << state.next_input_to_read << std::endl;
            algorithm.consume(state.input_chunk, state.next_input_to_read);
            state.has_input = false;
        }
        else if (state.no_data)
        {
            if (empty_chunk_on_finish)
            {
                IMergingAlgorithm::Input current_input;
                algorithm.consume(current_input, state.next_input_to_read);
            }
            else
            {
                /// The required source finished without data. Let the algorithm release any
                /// per-source bookkeeping (e.g. a read-ahead slot held for a deferred source).
                algorithm.onSourceExhausted(state.next_input_to_read);
            }
            state.no_data = false;
        }

        /// Taken only now, after the data the merge asked for along with the chunk has arrived:
        /// the first rows of that data may precede everything else the merge holds.
        if (virtual_row_announcement_pending)
        {
            virtual_row_announcement_pending = false;
            if (announceVirtualRow())
                return;
        }

        IMergingAlgorithm::Status status = algorithm.merge();

        if ((status.chunk && status.chunk.hasRows()) || !status.chunk.getChunkInfos().empty())
        {
            // std::cerr << "Got chunk with " << status.chunk.getNumRows() << " rows" << std::endl;
            state.output_chunk = std::move(status.chunk);

            if (announce_virtual_row_per_block && !status.is_finished && state.output_chunk.hasRows())
                virtual_row_announcement_pending = true;
        }

        if (status.required_source >= 0)
        {
            // std::cerr << "Required data for input " << status.required_source << std::endl;
            state.next_input_to_read = status.required_source;
            state.need_data = true;
        }

        if (!status.sources_to_prefetch.empty())
            state.inputs_to_prefetch.insert(
                state.inputs_to_prefetch.end(), status.sources_to_prefetch.begin(), status.sources_to_prefetch.end());

        if (status.is_finished)
        {
            // std::cerr << "Finished" << std::endl;
            state.is_finished = true;
        }

        merging_elapsed_ns += watch.elapsedNanoseconds();
    }

protected:
    /// Call `consume` with empty chunk when there is no more data.
    bool empty_chunk_on_finish = false;

    Algorithm algorithm;

    /// Puts the algorithm's bound into the output as a virtual row. False when there is none to announce.
    bool announceVirtualRow()
    {
        auto info = algorithm.getVirtualRowForNextOutput();
        if (!info)
            return false;

        Chunk chunk(outputs.front().getHeader().cloneEmptyColumns(), 0);
        chunk.getChunkInfos().add(std::move(info));
        state.output_chunk = std::move(chunk);
        return true;
    }

    /// Profile info.
    UInt64 merging_elapsed_ns = 0;

    void logMergedStats(ProfileEvents::Event elapsed_ms_event, std::string_view transform_message, LoggerPtr log) const
    {
        auto stats = algorithm.getMergedStats();

        UInt64 elapsed_ms = merging_elapsed_ns / 1000000LL;
        ProfileEvents::increment(elapsed_ms_event, elapsed_ms);

        /// Don't print info for small parts (< 1M rows)
        if (stats.rows < 1000000)
            return;

        double seconds = static_cast<double>(merging_elapsed_ns) / 1000000000ULL;

        if (seconds == 0.0)
        {
            LOG_DEBUG(log, "{}, {} blocks, {} rows, {} bytes in 0 sec.",
                transform_message, stats.blocks, stats.rows, stats.bytes);
        }
        else
        {
            LOG_DEBUG(log, "{}, {} blocks, {} rows, {} bytes in {:.3f} sec., {:.3f} rows/sec., {}/sec.",
                transform_message, stats.blocks, stats.rows, stats.bytes,
                seconds, static_cast<double>(stats.rows) / seconds, ReadableSize(static_cast<double>(stats.bytes) / seconds));
        }
    }

private:
    using IMergingTransformBase::state;
};

using MergingTransformPtr = std::shared_ptr<IMergingTransformBase>;

}
