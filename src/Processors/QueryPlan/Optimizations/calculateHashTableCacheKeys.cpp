#include <functional>
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
#include <Common/logger_useful.h>

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
    {
        /// Hash the filter-expression subtree only (rooted at `prewhere_column_name`), and strip
        /// `__applyFilter(_runtime_filter_*, ...)` contributions along the way. The runtime filter
        /// is pushed into the prewhere only on the probe side of a JOIN, so its presence depends on
        /// which join side the DP optimizer happened to route this table to. The underlying storage
        /// read is the same either way — see the matching transparency treatment in the
        /// `FilterStep` path below.
        ///
        /// We hash only the filter subtree rather than the whole `prewhere_actions` DAG because the
        /// DAG's output list also carries pass-through columns that the downstream plan consumes;
        /// those pass-through outputs differ between the two plan builds exactly because the probe
        /// side's filter DAG has to propagate its extra input through to the downstream JOIN.
        /// Hashing just the filter expression sidesteps that bookkeeping and captures the
        /// cost-relevant part of the prewhere.
        auto is_runtime_filter = [](const ActionsDAG::Node * n)
        {
            return n && n->type == ActionsDAG::ActionType::FUNCTION && n->function_base
                && n->function_base->getName() == "__applyFilter";
        };
        std::function<void(const ActionsDAG::Node *, SipHash &)> hash_filter_expr
            = [&](const ActionsDAG::Node * node, SipHash & h)
        {
            if (is_runtime_filter(node))
                return;

            /// `and(..., __applyFilter(...), ...)` simplifies to the AND of the remaining
            /// conditions; if only one non-runtime-filter child is left, the AND collapses to that
            /// child; if none are left, the filter becomes trivially true and we emit nothing.
            if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base
                && node->function_base->getName() == "and")
            {
                std::vector<const ActionsDAG::Node *> survivors;
                survivors.reserve(node->children.size());
                for (const auto * child : node->children)
                    if (!is_runtime_filter(child))
                        survivors.push_back(child);

                if (survivors.empty())
                    return;
                if (survivors.size() == 1)
                {
                    hash_filter_expr(survivors.front(), h);
                    return;
                }
                h.update(uint8_t(node->type));
                h.update(node->function_base->getName());
                for (const auto * child : survivors)
                    hash_filter_expr(child, h);
                return;
            }

            h.update(uint8_t(node->type));
            h.update(node->result_name);
            if (node->result_type)
                h.update(node->result_type->getName());
            if (node->function_base)
                h.update(node->function_base->getName());
            if (node->column)
                h.update(node->column->getName());
            for (const auto * child : node->children)
                hash_filter_expr(child, h);
        };

        const auto * filter_root = dag->prewhere_actions.tryFindInOutputs(dag->prewhere_column_name);
        if (filter_root)
            hash_filter_expr(filter_root, hash);
    }
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

        /// Canonicalize `JoinStep` children order so DP-driven side swaps don't cause the subtree
        /// hash to diverge between the single-replica and parallel-replicas plan builds in
        /// `considerEnablingParallelReplicas`. For commutative kinds (`INNER`/`FULL`/`CROSS`/`Comma`)
        /// sort children by their cache key. For `LEFT`/`RIGHT` rely on the equivalence
        /// `A LEFT JOIN B ≡ B RIGHT JOIN A`: emit the children in `LEFT`-anchored order by swapping
        /// them when the step is a `RIGHT` JOIN. Other kinds keep their existing order.
        if (const auto * join_step = dynamic_cast<const JoinStep *>(node.step.get()); join_step && node.children.size() == 2)
        {
            const auto kind = join_step->getJoin()->getTableJoin().kind();
            auto a = cache_keys[node.children.at(0)];
            auto b = cache_keys[node.children.at(1)];
            if (isInner(kind) || isFull(kind) || isCrossOrComma(kind))
            {
                if (a > b)
                    std::swap(a, b);
            }
            else if (isRight(kind))
            {
                std::swap(a, b);
            }
            frame.hash.update(a);
            frame.hash.update(b);
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

        {
            std::string child_str;
            for (const auto * c : node.children)
                child_str += fmt::format(" c={}", cache_keys[c]);
            LOG_DEBUG(getLogger("hk"), "{}{} -> {}", node.step->getName(), child_str, cache_keys[&node]);
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
