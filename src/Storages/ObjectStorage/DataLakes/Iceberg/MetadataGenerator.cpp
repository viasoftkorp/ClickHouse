#include <IO/ReadHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>

#include <climits>
#include <string_view>
#include <type_traits>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/JSON/Parser.h>

#include <Common/randomSeed.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <base/types.h>

#if USE_AVRO

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


namespace DB
{

namespace
{

Poco::JSON::Object::Ptr deepCopy(Poco::JSON::Object::Ptr obj)
{
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    obj->stringify(oss);

    Poco::JSON::Parser parser;
    auto result = parser.parse(oss.str());
    return result.extract<Poco::JSON::Object::Ptr>();
}

bool checkValidSchemaEvolution(Poco::Dynamic::Var old_type, Poco::Dynamic::Var new_type)
{
    if (old_type.isString() && new_type.isString() && old_type.extract<String>() == new_type.extract<String>())
        return true;

    if (new_type.isString() && new_type.extract<String>() == "long" &&
        old_type.isString() && (old_type.extract<String>() == "long" ||  old_type.extract<String>() == "int"))
    {
        return true;
    }

    if (new_type.isString() && new_type.extract<String>() == "double" &&
        old_type.isString() && (old_type.extract<String>() == "float" ||  old_type.extract<String>() == "double"))
    {
        return true;
    }

    {
        auto old_complex_type = old_type.extract<Poco::JSON::Object::Ptr>();
        auto new_complex_type = new_type.extract<Poco::JSON::Object::Ptr>();

        if (old_complex_type && new_complex_type && old_complex_type->has("precision") && new_complex_type->has("precision") &&
            (old_complex_type->getValue<Int32>("precision") <= new_complex_type->getValue<Int32>("precision") &&
             old_complex_type->getValue<Int32>("scale") <= new_complex_type->getValue<Int32>("scale")))
        {
            return true;
        }
    }

    return false;
}

}

MetadataGenerator::MetadataGenerator(Poco::JSON::Object::Ptr metadata_object_)
    : metadata_object(metadata_object_)
    , gen(randomSeed())
    , dis(1, std::numeric_limits<Int64>::max())
{
}

Int64 MetadataGenerator::getMaxSequenceNumber()
{
    /// Use the authoritative top-level field per Iceberg V2 spec.
    /// Iterating snapshots is unreliable when catalogs prune snapshot history.
    if (metadata_object->has(Iceberg::f_last_sequence_number))
        return metadata_object->getValue<Int64>(Iceberg::f_last_sequence_number);

    auto snapshots = metadata_object->get(Iceberg::f_snapshots).extract<Poco::JSON::Array::Ptr>();
    Int64 max_seq_number = 0;

    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        auto seq_number = snapshot->getValue<Int64>(Iceberg::f_metadata_sequence_number);
        max_seq_number = std::max(max_seq_number, seq_number);
    }
    return max_seq_number;
}

Poco::JSON::Object::Ptr MetadataGenerator::getParentSnapshot(Int64 parent_snapshot_id)
{
    auto snapshots = metadata_object->get(Iceberg::f_snapshots).extract<Poco::JSON::Array::Ptr>();
    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        auto snapshot_id = snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
        if (snapshot_id == parent_snapshot_id)
            return snapshot;
    }
    return nullptr;
}

MetadataGenerator::NextMetadataResult MetadataGenerator::generateNextMetadata(
    FileNamesGenerator & generator,
    const Iceberg::IcebergPathFromMetadata & metadata_file_path,
    Int64 parent_snapshot_id,
    SnapshotSummary snapshot_summary,
    std::optional<Int64> user_defined_snapshot_id,
    std::optional<Int64> user_defined_timestamp)
{
    int format_version = metadata_object->getValue<Int32>(Iceberg::f_format_version);
    Poco::JSON::Object::Ptr new_snapshot = new Poco::JSON::Object;
    if (format_version > 1)
    {
        auto sequence_number = getMaxSequenceNumber() + 1;
        new_snapshot->set(Iceberg::f_metadata_sequence_number, sequence_number);
        metadata_object->set(Iceberg::f_last_sequence_number, sequence_number);
    }
    Int64 snapshot_id = user_defined_snapshot_id.value_or(static_cast<Int64>(dis(gen)));

    auto manifest_list_path = generator.generateManifestListName(snapshot_id, format_version);
    new_snapshot->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
    new_snapshot->set(Iceberg::f_parent_snapshot_id, parent_snapshot_id);

    auto now = std::chrono::system_clock::now();
    auto ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    Int64 timestamp = user_defined_timestamp.value_or(ms.count());
    new_snapshot->set(Iceberg::f_timestamp_ms, timestamp);
    metadata_object->set(Iceberg::f_last_updated_ms, timestamp);

    if (auto parent_snapshot = getParentSnapshot(parent_snapshot_id))
        snapshot_summary.applyTotalsFromParent(SnapshotSummary::parse(*parent_snapshot));

    Poco::JSON::Object::Ptr summary = new Poco::JSON::Object;

    snapshot_summary.fill(*summary);

    new_snapshot->set(Iceberg::f_summary, summary);

    new_snapshot->set(Iceberg::f_schema_id, metadata_object->getValue<Int32>(Iceberg::f_current_schema_id));
    new_snapshot->set(Iceberg::f_manifest_list, manifest_list_path.serialize());

    if (format_version >= 3)
    {
        Int64 next_row_id = metadata_object->has(Iceberg::f_next_row_id) && !metadata_object->isNull(Iceberg::f_next_row_id)
            ? metadata_object->getValue<Int64>(Iceberg::f_next_row_id)
            : 0;
        new_snapshot->set(Iceberg::f_first_row_id, next_row_id);
        new_snapshot->set(Iceberg::f_added_rows, added_records);
        metadata_object->set(Iceberg::f_next_row_id, next_row_id + added_records);
    }

    metadata_object->getArray(Iceberg::f_snapshots)->add(new_snapshot);
    metadata_object->set(Iceberg::f_current_snapshot_id, snapshot_id);

    if (!metadata_object->has(Iceberg::f_refs))
        metadata_object->set(Iceberg::f_refs, new Poco::JSON::Object);

    if (!metadata_object->getObject(Iceberg::f_refs)->has(Iceberg::f_main))
    {
        Poco::JSON::Object::Ptr branch = new Poco::JSON::Object;
        branch->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
        branch->set(Iceberg::f_type, Iceberg::f_branch);

        metadata_object->getObject(Iceberg::f_refs)->set(Iceberg::f_main, branch);
    }
    else
        metadata_object->getObject(Iceberg::f_refs)->getObject(Iceberg::f_main)->set(Iceberg::f_metadata_snapshot_id, snapshot_id);

    {
        Poco::JSON::Object::Ptr new_metadata_item = new Poco::JSON::Object;
        new_metadata_item->set(Iceberg::f_metadata_file, metadata_file_path.serialize());
        new_metadata_item->set(Iceberg::f_timestamp_ms, timestamp);
        metadata_object->getArray(Iceberg::f_metadata_log)->add(new_metadata_item);
    }
    {
        Poco::JSON::Object::Ptr new_snapshot_item = new Poco::JSON::Object;
        new_snapshot_item->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
        new_snapshot_item->set(Iceberg::f_timestamp_ms, timestamp);
        metadata_object->getArray(Iceberg::f_snapshot_log)->add(new_snapshot_item);
    }

    if (snapshot_summary.added_delete_files > 0)
    {
        if (!metadata_object->has(Iceberg::f_properties))
        {
            Poco::JSON::Object::Ptr properties = new Poco::JSON::Object;
            metadata_object->set(Iceberg::f_properties, properties);
        }
        auto properties = metadata_object->getObject(Iceberg::f_properties);
        properties->set("owner", "root");
        properties->set("write.delete.mode", "merge-on-read");
        properties->set("write.merge.mode", "merge-on-read");
        properties->set("write.update.mode", "merge-on-read");
    }
    return {new_snapshot, manifest_list_path};
}

void MetadataGenerator::generateDropColumnMetadata(const String & column_name)
{
    auto current_schema_id = metadata_object->getValue<Int32>(Iceberg::f_current_schema_id);
    metadata_object->set(Iceberg::f_current_schema_id, current_schema_id + 1);

    Poco::JSON::Object::Ptr current_schema;
    auto schemas = metadata_object->getArray(Iceberg::f_schemas);
    for (UInt32 i = 0; i < schemas->size(); ++i)
    {
        if (schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
        {
            current_schema = schemas->getObject(i);
            break;
        }
    }

    if (!current_schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found schema with id {}", current_schema_id);
    current_schema = deepCopy(current_schema);

    auto fields = current_schema->getArray(Iceberg::f_fields);
    UInt32 index_to_drop = static_cast<UInt32>(fields->size());
    for (UInt32 i = 0; i < fields->size(); ++i)
    {
        if (fields->getObject(i)->getValue<String>(Iceberg::f_name) == column_name)
        {
            index_to_drop = i;
            break;
        }
    }
    if (index_to_drop == fields->size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found column {}", column_name);
    current_schema->getArray(Iceberg::f_fields)->remove(index_to_drop);
    current_schema->set(Iceberg::f_schema_id, current_schema_id + 1);
    metadata_object->getArray(Iceberg::f_schemas)->add(current_schema);
}

void MetadataGenerator::generateAddColumnMetadata(const String & column_name, DataTypePtr type)
{
    if (!type->isNullable())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg spec doesn't allow to add non-nullable columns");
    auto current_schema_id = metadata_object->getValue<Int32>(Iceberg::f_current_schema_id);
    metadata_object->set(Iceberg::f_current_schema_id, current_schema_id + 1);

    Poco::JSON::Object::Ptr current_schema;
    auto schemas = metadata_object->getArray(Iceberg::f_schemas);
    for (UInt32 i = 0; i < schemas->size(); ++i)
    {
        if (schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
        {
            current_schema = schemas->getObject(i);
            break;
        }
    }

    if (!current_schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found schema with id {}", current_schema_id);
    current_schema = deepCopy(current_schema);
    auto last_column_id = metadata_object->getValue<Int32>(Iceberg::f_last_column_id);
    metadata_object->set(Iceberg::f_last_column_id, last_column_id + 1);

    auto new_type = Iceberg::getIcebergType(type, last_column_id);
    Poco::JSON::Object::Ptr new_field = new Poco::JSON::Object;
    new_field->set(Iceberg::f_id, last_column_id + 1);
    new_field->set(Iceberg::f_name, column_name);
    new_field->set(Iceberg::f_required, new_type.second);
    new_field->set(Iceberg::f_type, new_type.first);

    current_schema->getArray(Iceberg::f_fields)->add(new_field);
    current_schema->set(Iceberg::f_schema_id, current_schema_id + 1);
    metadata_object->getArray(Iceberg::f_schemas)->add(current_schema);
}

void MetadataGenerator::generateModifyColumnMetadata(const String & column_name, DataTypePtr type)
{
    auto current_schema_id = metadata_object->getValue<Int32>(Iceberg::f_current_schema_id);
    metadata_object->set(Iceberg::f_current_schema_id, current_schema_id + 1);

    Poco::JSON::Object::Ptr current_schema;
    auto schemas = metadata_object->getArray(Iceberg::f_schemas);
    for (UInt32 i = 0; i < schemas->size(); ++i)
    {
        if (schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
        {
            current_schema = schemas->getObject(i);
            break;
        }
    }

    if (!current_schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found schema with id {}", current_schema_id);
    current_schema = deepCopy(current_schema);
    auto last_column_id = metadata_object->getValue<Int32>(Iceberg::f_last_column_id);

    auto new_type = Iceberg::getIcebergType(type, last_column_id);
    auto schema_fields = current_schema->getArray(Iceberg::f_fields);

    for (UInt32 i = 0; i < schema_fields->size(); ++i)
    {
        auto current_field = schema_fields->getObject(i);
        if (current_field->getValue<String>(Iceberg::f_name) == column_name)
        {
            if (!checkValidSchemaEvolution(current_field->get(Iceberg::f_type), new_type.first))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg spec doesn't allow schema evolution to type {}", type->getPrettyName());

            auto old_type = deepCopy(current_field);
            current_field->set(Iceberg::f_type, new_type.first);
            if (!current_field->getValue<bool>(Iceberg::f_required) && !type->isNullable())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg spec doesn't allow change type from nullable to non-nullable {}", type->getPrettyName());

            current_field->set(Iceberg::f_required, new_type.second);
            break;
        }
    }
    current_schema->set(Iceberg::f_schema_id, current_schema_id + 1);
    metadata_object->getArray(Iceberg::f_schemas)->add(current_schema);
}

void MetadataGenerator::generateRenameColumnMetadata(const String & column_name, const String & new_column_name)
{
    auto current_schema_id = metadata_object->getValue<Int32>(Iceberg::f_current_schema_id);

    Poco::JSON::Object::Ptr current_schema;
    auto schemas = metadata_object->getArray(Iceberg::f_schemas);
    for (UInt32 i = 0; i < schemas->size(); ++i)
    {
        if (schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
        {
            current_schema = schemas->getObject(i);
            break;
        }
    }

    if (!current_schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found schema with id {}", current_schema_id);
    current_schema = deepCopy(current_schema);

    auto schema_fields = current_schema->getArray(Iceberg::f_fields);

    for (UInt32 i = 0; i < schema_fields->size(); ++i)
    {
        if (schema_fields->getObject(i)->getValue<String>(Iceberg::f_name) == new_column_name)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column {} already exists", new_column_name);
    }

    bool found = false;
    for (UInt32 i = 0; i < schema_fields->size(); ++i)
    {
        auto current_field = schema_fields->getObject(i);
        if (current_field->getValue<String>(Iceberg::f_name) == column_name)
        {
            current_field->set(Iceberg::f_name, new_column_name);
            found = true;
            break;
        }
    }

    if (!found)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found column {}", column_name);

    metadata_object->set(Iceberg::f_current_schema_id, current_schema_id + 1);
    current_schema->set(Iceberg::f_schema_id, current_schema_id + 1);
    metadata_object->getArray(Iceberg::f_schemas)->add(current_schema);
}

void MetadataGenerator::SnapshotSummary::applyTotalsFromParent(const SnapshotSummary & parent)
{
    switch (operation)
    {
        case Operation::APPEND:
        case Operation::OVERWRITE:
            total_records = parent.total_records + added_records;
            total_files_size = parent.total_files_size + added_files_size;
            total_data_files = parent.total_data_files + added_files;
            total_delete_files = parent.total_delete_files + added_delete_files;
            total_position_deletes = parent.total_position_deletes + num_deleted_rows;
            total_equality_deletes = parent.total_equality_deletes;
            break;
        case Operation::DELETE:
            total_records = parent.total_records - removed_records;
            total_files_size = parent.total_files_size - removed_files_size;
            total_data_files = parent.total_data_files - removed_data_files;
            total_delete_files = parent.total_delete_files - removed_position_delete_files;
            total_position_deletes = parent.total_position_deletes - removed_position_deletes;
            total_equality_deletes = parent.total_equality_deletes;
            break;
    }
}

void MetadataGenerator::SnapshotSummary::fill(Poco::JSON::Object & obj) const
{
    /// https://iceberg.apache.org/spec/?h=summary#optional-snapshot-summary-fields
    /// Snapshot summary can include metrics fields to track numeric stats of the snapshot (see Metrics) and operational details (see Other Fields).
    /// The value of these fields should be of string type (e.g., "120").
    auto set_as_string = [&](const char * field, Int64 val)
    {
        obj.set(field, std::to_string(val));
    };

    switch (operation)
    {
        case Operation::APPEND:
        {
            chassert(num_deleted_rows == 0);
            obj.set(Iceberg::f_operation, Iceberg::f_append);
            set_as_string(Iceberg::f_added_data_files, added_files);
            set_as_string(Iceberg::f_added_records, added_records);
            set_as_string(Iceberg::f_added_files_size, added_files_size);
            set_as_string(Iceberg::f_changed_partition_count, num_partitions);
            break;
        }
        case Operation::OVERWRITE:
        {
            chassert(num_deleted_rows != 0);
            obj.set(Iceberg::f_operation, Iceberg::f_overwrite);
            set_as_string(Iceberg::f_added_delete_files, added_delete_files);
            set_as_string(Iceberg::f_added_position_delete_files, added_delete_files);
            set_as_string(Iceberg::f_added_files_size, added_files_size);
            set_as_string(Iceberg::f_added_position_deletes, num_deleted_rows);
            set_as_string(Iceberg::f_changed_partition_count, num_partitions);
            break;
        }
        case Operation::DELETE:
        {
            obj.set(Iceberg::f_operation, Iceberg::f_delete);
            set_as_string(Iceberg::f_removed_data_files, removed_data_files);
            set_as_string(Iceberg::f_deleted_data_files, removed_data_files);
            set_as_string(Iceberg::f_deleted_records, removed_records);
            set_as_string(Iceberg::f_removed_files_size, removed_files_size);
            if (removed_position_delete_files > 0)
                set_as_string(Iceberg::f_removed_position_delete_files, removed_position_delete_files);
            set_as_string(Iceberg::f_changed_partition_count, num_partitions);
            break;
        }
    }

    set_as_string(Iceberg::f_total_records, total_records);
    set_as_string(Iceberg::f_total_files_size, total_files_size);
    set_as_string(Iceberg::f_total_data_files, total_data_files);
    set_as_string(Iceberg::f_total_delete_files, total_delete_files);
    set_as_string(Iceberg::f_total_position_deletes, total_position_deletes);
    set_as_string(Iceberg::f_total_equality_deletes, total_equality_deletes);
}

MetadataGenerator::SnapshotSummary MetadataGenerator::SnapshotSummary::createAppend(
    Int64 added_files, Int64 added_records, Int64 added_files_size, Int64 num_partitions)
{
    SnapshotSummary result;
    result.operation = Operation::APPEND;
    result.added_files = added_files;
    result.added_records = added_records;
    result.added_files_size = added_files_size;
    result.num_partitions = num_partitions;
    return result;
}

MetadataGenerator::SnapshotSummary MetadataGenerator::SnapshotSummary::createOverwrite(
    Int64 added_delete_files, Int64 added_files_size, Int64 num_partitions, Int64 num_deleted_rows)
{
    SnapshotSummary result;
    result.operation = Operation::OVERWRITE;
    result.added_delete_files = added_delete_files;
    result.added_files_size = added_files_size;
    result.num_partitions = num_partitions;
    result.num_deleted_rows = num_deleted_rows;
    return result;
}

MetadataGenerator::SnapshotSummary MetadataGenerator::SnapshotSummary::createDelete(
    Int64 removed_data_files,
    Int64 removed_records,
    Int64 removed_files_size,
    Int64 removed_position_delete_files,
    Int64 removed_position_deletes,
    Int64 num_partitions)
{
    SnapshotSummary result;
    result.operation = Operation::DELETE;
    result.removed_data_files = removed_data_files;
    result.removed_records = removed_records;
    result.removed_files_size = removed_files_size;
    result.removed_position_delete_files = removed_position_delete_files;
    result.removed_position_deletes = removed_position_deletes;
    result.num_partitions = num_partitions;
    return result;
}

MetadataGenerator::SnapshotSummary MetadataGenerator::SnapshotSummary::parse(const Poco::JSON::Object & obj)
{
    SnapshotSummary result;

    const auto operation_str = obj.getValue<String>(Iceberg::f_operation);
    if (operation_str == Iceberg::f_append)
        result.operation = Operation::APPEND;
    else if (operation_str == Iceberg::f_overwrite)
        result.operation = Operation::OVERWRITE;
    else if (operation_str == Iceberg::f_delete)
        result.operation = Operation::DELETE;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown snapshot summary operation: {}", operation_str);

    /// Iceberg spec stores all summary metric values as strings (e.g., "120").
    auto get_optional_int = [&](const char * field) -> Int64
    {
        if (!obj.has(field))
            return 0;
        return DB::parse<Int64>(obj.getValue<String>(field));
    };

    result.added_files = get_optional_int(Iceberg::f_added_data_files);
    result.added_records = get_optional_int(Iceberg::f_added_records);
    result.added_files_size = get_optional_int(Iceberg::f_added_files_size);
    result.num_partitions = get_optional_int(Iceberg::f_changed_partition_count);
    result.added_delete_files = get_optional_int(Iceberg::f_added_delete_files);
    result.num_deleted_rows = get_optional_int(Iceberg::f_added_position_deletes);
    result.removed_data_files = get_optional_int(Iceberg::f_removed_data_files);
    result.removed_records = get_optional_int(Iceberg::f_deleted_records);
    result.removed_files_size = get_optional_int(Iceberg::f_removed_files_size);
    result.removed_position_delete_files = get_optional_int(Iceberg::f_removed_position_delete_files);

    result.total_records = get_optional_int(Iceberg::f_total_records);
    result.total_files_size = get_optional_int(Iceberg::f_total_files_size);
    result.total_data_files = get_optional_int(Iceberg::f_total_data_files);
    result.total_delete_files = get_optional_int(Iceberg::f_total_delete_files);
    result.total_position_deletes = get_optional_int(Iceberg::f_total_position_deletes);
    result.total_equality_deletes = get_optional_int(Iceberg::f_total_equality_deletes);

    return result;
}
}

#endif
