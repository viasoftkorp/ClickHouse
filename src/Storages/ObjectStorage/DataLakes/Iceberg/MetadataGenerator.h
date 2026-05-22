#pragma once

#include <pcg_random.hpp>
#include "config.h"

#include <DataTypes/IDataType.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Poco/JSON/Object.h>


namespace DB
{

#if USE_AVRO

class MetadataGenerator
{
public:
    explicit MetadataGenerator(Poco::JSON::Object::Ptr metadata_object_);

    struct NextMetadataResult
    {
        Poco::JSON::Object::Ptr snapshot = nullptr;
        /// Metadata path for the manifest list file (e.g. "wasb://container@account/table/metadata/snap-xxx.avro").
        /// Use IcebergPathResolver::resolve to get storage path for I/O.
        /// Use .serialize() to get the path for writing into Iceberg metadata.
        Iceberg::IcebergPathFromMetadata manifest_list_path;
    };

    struct SnapshotSummary
    {
        enum class Operation
        {
            APPEND,
            OVERWRITE,
            DELETE
        };

        Operation operation;

        Int64 added_files = 0;
        Int64 added_records = 0;
        Int64 added_files_size = 0;
        Int64 num_partitions = 0;
        Int64 added_delete_files = 0;
        Int64 num_deleted_rows = 0;
        Int64 removed_data_files = 0;
        Int64 removed_records = 0;
        Int64 removed_files_size = 0;
        Int64 removed_position_delete_files = 0;
        Int64 removed_position_deletes = 0;

        Int64 total_records = 0;
        Int64 total_files_size = 0;
        Int64 total_data_files = 0;
        Int64 total_delete_files = 0;
        Int64 total_position_deletes = 0;
        Int64 total_equality_deletes = 0;

        bool finalized = false;

        void finalize(std::optional<SnapshotSummary> parent);

        Poco::JSON::Object::Ptr toJSON() const;

        static SnapshotSummary fromJSON(const Poco::JSON::Object & obj);

        static SnapshotSummary createAppend(
            Int64 added_files,
            Int64 added_records,
            Int64 added_files_size,
            Int64 num_partitions);

        static SnapshotSummary createOverwrite(
            Int64 added_delete_files,
            Int64 added_files_size,
            Int64 num_partitions,
            Int64 num_deleted_rows);

        static SnapshotSummary createDelete(
            Int64 removed_data_files,
            Int64 removed_records,
            Int64 removed_files_size,
            Int64 removed_position_delete_files,
            Int64 removed_position_deletes,
            Int64 num_partitions);
    };

    NextMetadataResult generateNextMetadata(
        FileNamesGenerator & generator,
        const Iceberg::IcebergPathFromMetadata & metadata_file_path,
        Int64 parent_snapshot_id,
        SnapshotSummary snapshot_summary,
        std::optional<Int64> user_defined_snapshot_id = std::nullopt,
        std::optional<Int64> user_defined_timestamp = std::nullopt);

    void generateAddColumnMetadata(const String & column_name, DataTypePtr type);
    void generateDropColumnMetadata(const String & column_name);
    void generateModifyColumnMetadata(const String & column_name, DataTypePtr type);
    void generateRenameColumnMetadata(const String & column_name, const String & new_column_name);

private:
    Poco::JSON::Object::Ptr metadata_object;

    pcg64_fast gen;
    std::uniform_int_distribution<Int64> dis;

    Int64 getMaxSequenceNumber();
    Poco::JSON::Object::Ptr getParentSnapshot(Int64 parent_snapshot_id);
};

#endif

}
