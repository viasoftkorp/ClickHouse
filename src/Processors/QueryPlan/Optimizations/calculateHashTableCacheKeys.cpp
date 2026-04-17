#include <unordered_map>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Core/Joins.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/SetSerialization.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/IStorage.h>
#include <Common/SipHash.h>

using namespace DB;

namespace
{

UInt64 calculateHashFromStep(const ReadFromParallelRemoteReplicasStep & source)
{
    SipHash hash;
    hash.update(source.getSerializationName());
    hash.update(source.getStorageID().getFullTableName());
    return hash.get64();
}

UInt64 calculateHashFromStep(const SourceStepWithFilter & read)
{
    SipHash hash;
    hash.update(read.getSerializationName());
    String table_name;
    if (const auto & snapshot = read.getStorageSnapshot())
    {
        StorageID storage_id = snapshot->storage.getStorageID();
        if (storage_id.hasUUID())
            hash.update(storage_id.uuid.toUnderType());
        else
            hash.update(storage_id.getFullTableName());
        table_name = storage_id.getFullTableName();
    }
    if (const auto & dag = read.getPrewhereInfo())
        dag->prewhere_actions.updateHash(hash);
    return hash.get64();
}

UInt64 calculateHashFromStep(const ITransformingStep & transform)
{
    // The purpose of `HashTablesStatistics` is to provide cardinality estimations.
    // Steps that preserve the number of input rows do not affect cardinality, so we can skip them.
    if (transform.getTransformTraits().preserves_number_of_rows)
        return 0;

    /// Runtime-join-filter `FilterStep`s (where the only output is `__applyFilter(runtime_filter_*,
    /// ...)`) are moved between children of a `JoinStep` when the DP optimizer swaps sides. They
    /// carry no information that differentiates two plans — just a bloom-filter test on a join key
    /// — so from hashing's perspective they are equivalent to an `ExpressionStep` doing the same
    /// column aliasing. Treat them the same way `ExpressionStep` is treated (contribute nothing to
    /// the parent hash) so that a subtree on one side of the JOIN in one plan matches the
    /// equivalent subtree on the opposite side of the JOIN in the other plan.
    if (const auto * filter = typeid_cast<const FilterStep *>(&transform))
    {
        const auto & dag = filter->getExpression();
        const auto * filter_node = dag.tryFindInOutputs(filter->getFilterColumnName());
        if (filter_node && filter_node->type == ActionsDAG::ActionType::FUNCTION && filter_node->function_base
            && filter_node->function_base->getName() == "__applyFilter")
            return 0;
    }

    WriteBufferFromOwnString wbuf;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{.out = wbuf, .registry = registry, .skip_final_flag = true, .skip_cache_key = true};

    writeStringBinary(transform.getSerializationName(), wbuf);
    if (transform.isSerializable())
        transform.serialize(ctx);

    SipHash hash;
    hash.update(wbuf.str());
    return hash.get64();
}

}

namespace DB
{

namespace QueryPlanOptimizations
{

UInt64 calculateJoinStepCacheKeyContribution(const JoinStepLogical & join_step, JoinTableSide side)
{
    SipHash hash;

    hash.update(join_step.getSerializationName());
    for (const auto & condition : join_step.getJoinOperator().expression)
    {
        auto [op, lhs, rhs] = condition.asBinaryPredicate();
        if (op == JoinConditionOperator::Equals || op == JoinConditionOperator::NullSafeEquals)
        {
            if (side == JoinTableSide::Left && lhs.fromLeft())
                lhs.getNode()->updateHash(hash);
            if (side == JoinTableSide::Left && rhs.fromLeft())
                rhs.getNode()->updateHash(hash);
            if (side == JoinTableSide::Right && lhs.fromRight())
                lhs.getNode()->updateHash(hash);
            if (side == JoinTableSide::Right && rhs.fromRight())
                rhs.getNode()->updateHash(hash);
        }
    }

    return hash.get64();
}

void calculateHashTableCacheKeys(
    const QueryPlan::Node & root,
    std::unordered_map<const QueryPlan::Node *, UInt64> & cache_keys,
    std::unordered_map<const QueryPlan::Node *, UInt64> & raw_hashes)
{
    struct Frame
    {
        const QueryPlan::Node * node = nullptr;
        size_t next_child = 0;
        // Hash state which steps should update with their own hashes
        SipHash hash{};
    };

    // We use addresses of `left` and `right`, so they should be stable
    std::list<Frame> stack;
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        auto & frame = stack.back();
        const auto & node = *frame.node;

        if (auto * join_step = dynamic_cast<JoinStepLogical *>(node.step.get()))
        {
            // `HashTablesStatistics` is used currently only for `parallel_hash_join`, i.e. the following calculation doesn't make sense for other join algorithms.
            const auto & join_expression = join_step->getJoinOperator().expression;
            bool single_disjunct = join_expression.size() > 1 || (join_expression.size() == 1 && !join_expression.front().isFunction(JoinConditionOperator::Or));
            const bool calculate = allowParallelHashJoin(
                join_step->getJoinSettings().join_algorithms,
                join_step->getJoinOperator().kind,
                join_step->getJoinOperator().strictness,
                typeid_cast<JoinStepLogicalLookup *>(node.children.back()->step.get()),
                single_disjunct);

            chassert(node.children.size() == 2);

            if (calculate)
            {
                if (frame.next_child == 0)
                {
                    frame.next_child = node.children.size();
                    stack.push_back({.node = node.children.at(0)});
                    stack.push_back({.node = node.children.at(1)});
                }
                else
                {
                    /// At this point cache_keys[child_i] holds the child's raw bottom-up hash
                    /// (set when the child's frame was popped). Apply this join's per-side
                    /// contribution to produce the child's final cache key, then SipHash-combine
                    /// the two final-keyed children to derive this join's own raw hash.
                    cache_keys[node.children.at(0)] ^= calculateJoinStepCacheKeyContribution(*join_step, JoinTableSide::Left);
                    cache_keys[node.children.at(1)] ^= calculateJoinStepCacheKeyContribution(*join_step, JoinTableSide::Right);
                    frame.hash.update(cache_keys[node.children.at(0)]);
                    frame.hash.update(cache_keys[node.children.at(1)]);
                    const auto raw = frame.hash.get64();
                    raw_hashes[&node] = raw;
                    cache_keys[&node] = raw;

                    stack.pop_back();
                }

                continue;
            }
        }

        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child], .hash = frame.hash};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }

        /// For a JOIN whose children are semantically interchangeable (INNER/FULL/CROSS/Comma), hash
        /// its children symmetrically so that a DP-driven swap that only reorders the children — but
        /// does not rename columns or rewrite predicates — does not cause the subtree hash to diverge
        /// between the single-replica plan and the parallel-replicas plan built by
        /// `considerEnablingParallelReplicas`. Outer joins (`LEFT`/`RIGHT`) remain order-sensitive
        /// because the DP optimizer flips the kind when it swaps their sides, and encoding that joint
        /// canonicalization is not needed for the current tests.
        if (const auto * join_step = dynamic_cast<const JoinStep *>(node.step.get()); join_step && node.children.size() == 2)
        {
            const auto kind = join_step->getJoin()->getTableJoin().kind();
            if (isInner(kind) || isFull(kind) || isCrossOrComma(kind))
            {
                auto a = cache_keys[node.children.at(0)];
                auto b = cache_keys[node.children.at(1)];
                if (a > b)
                    std::swap(a, b);
                frame.hash.update(a);
                frame.hash.update(b);
            }
            else
            {
                for (const auto * child : node.children)
                    frame.hash.update(cache_keys[child]);
            }
        }
        else
        {
            for (const auto * child : node.children)
                frame.hash.update(cache_keys[child]);
        }

        if (const auto * source = dynamic_cast<const ReadFromParallelRemoteReplicasStep *>(node.step.get()))
            frame.hash.update(calculateHashFromStep(*source));
        else if (const auto * read = dynamic_cast<const SourceStepWithFilter *>(node.step.get()))
            frame.hash.update(calculateHashFromStep(*read));
        else if (const auto * transform = dynamic_cast<const ITransformingStep *>(node.step.get()))
            // Completely ignore the ignored steps (i.e. the ones for which we return 0)
            if (auto hash = calculateHashFromStep(*transform))
                frame.hash.update(hash);

        const auto raw = frame.hash.get64();
        raw_hashes[&node] = raw;
        cache_keys[&node] = raw;

        /// Any transforming step that preserves the number of rows carries no cost-relevant
        /// information for `HashTablesStatistics` — it's pure column-level rewriting. Make those
        /// steps fully transparent so that structural differences between the two plan builds
        /// that only add/remove such steps (e.g. a probe-side `Filter(__applyFilter)` vs a
        /// build-side `Expression(Change col)` + `BuildRuntimeFilter`) collapse to the same
        /// subtree cache key on both sides of a DP join swap.
        if (const auto * transform = dynamic_cast<const ITransformingStep *>(node.step.get()))
        {
            chassert(node.children.size() == 1);
            const bool is_runtime_filter = [&]
            {
                const auto * filter = typeid_cast<const FilterStep *>(node.step.get());
                if (!filter)
                    return false;
                const auto * filter_node = filter->getExpression().tryFindInOutputs(filter->getFilterColumnName());
                return filter_node && filter_node->type == ActionsDAG::ActionType::FUNCTION && filter_node->function_base
                    && filter_node->function_base->getName() == "__applyFilter";
            }();
            if (transform->getTransformTraits().preserves_number_of_rows || is_runtime_filter)
                cache_keys[&node] = cache_keys[node.children.front()];
        }

        stack.pop_back();
    }
}

void calculateHashTableCacheKeys(const QueryPlan::Node & root, std::unordered_map<const QueryPlan::Node *, UInt64> & cache_keys)
{
    std::unordered_map<const QueryPlan::Node *, UInt64> raw_hashes;
    calculateHashTableCacheKeys(root, cache_keys, raw_hashes);
}

std::unordered_map<const QueryPlan::Node *, UInt64> calculateHashTableCacheKeys(const QueryPlan::Node & root)
{
    std::unordered_map<const QueryPlan::Node *, UInt64> cache_keys;
    calculateHashTableCacheKeys(root, cache_keys);
    return cache_keys;
}

}
}
