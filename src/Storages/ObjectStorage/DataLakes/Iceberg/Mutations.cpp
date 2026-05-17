#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Common/FieldAccurateComparison.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DataLake/Common.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileIterator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Formats/FormatFactory.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTPartition.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/AlterCommands.h>
#include <Storages/MutationCommands.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Mutations.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/PartitionCommands.h>
#include <Storages/VirtualColumnUtils.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <limits>
#include <set>
#include <unordered_set>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int LIMIT_EXCEEDED;
extern const int NOT_IMPLEMENTED;
extern const int SUPPORT_IS_DISABLED;
}

namespace DB::DataLakeStorageSetting
{
extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
}

namespace DB::FailPoints
{
extern const char iceberg_writes_cleanup[];
}

namespace DB::Setting
{
extern const SettingsBool allow_insert_into_iceberg;
}

namespace DB::Iceberg
{

#if USE_AVRO

static constexpr const char * block_datafile_path = "_iceberg_metadata_file_path";
static constexpr const char * block_row_number = "_row_number";
static constexpr auto MAX_TRANSACTION_RETRIES = 100;

struct DeleteFileWriteResult
{
    /// Metadata path (e.g. "wasb://container@account/table/data/uuid-deletes.parquet")
    Iceberg::IcebergPathFromMetadata path;
    Int64 total_rows;
    Int64 total_bytes;
};

using DataFileWriteResultByPartitionKey = std::unordered_map<ChunkPartitioner::PartitionKey, DeleteFileWriteResult, ChunkPartitioner::PartitionKeyHasher>;
using DataFileStatisticsByPartitionKey = std::unordered_map<ChunkPartitioner::PartitionKey, DataFileStatistics, ChunkPartitioner::PartitionKeyHasher>;

struct DataFileWriteResultWithStats
{
    DataFileWriteResultByPartitionKey delete_file;
    DataFileStatisticsByPartitionKey delete_statistic;
};

struct WriteDataFilesResult
{
    DataFileWriteResultWithStats delete_file;
    std::optional<DataFileWriteResultWithStats> data_file;
};

static Block getPositionDeleteFileSampleBlock()
{
    ColumnsWithTypeAndName delete_file_columns_desc;
    delete_file_columns_desc.push_back(
        ColumnWithTypeAndName(std::make_shared<DataTypeString>(), IcebergPositionDeleteTransform::data_file_path_column_name));
    delete_file_columns_desc.push_back(
        ColumnWithTypeAndName(std::make_shared<DataTypeInt64>(), IcebergPositionDeleteTransform::positions_column_name));

    return Block(delete_file_columns_desc);
}

static Block getNonVirtualColumns(const Block & block, bool remove_low_cardinality = false)
{
    auto virtual_columns_desc = VirtualColumnUtils::getVirtualNamesForFileLikeStorage();
    std::unordered_set<String> virtual_columns;
    for (const auto & column_desc : virtual_columns_desc)
        virtual_columns.insert(column_desc);
    ColumnsWithTypeAndName columns;
    for (size_t i = 0; i < block.getNames().size(); ++i)
    {
        if (virtual_columns.contains(block.getNames()[i]))
            continue;
        auto col_type = block.getDataTypes()[i];
        auto col_data = block.getColumns()[i];
        if (remove_low_cardinality)
        {
            col_type = removeLowCardinality(col_type);
            col_data = col_data->convertToFullColumnIfLowCardinality();
        }
        columns.push_back(ColumnWithTypeAndName(col_data, col_type, block.getNames()[i]));
    }
    return Block(columns);
}

static std::vector<std::pair<ChunkPartitioner::PartitionKey, Chunk>>
getPartitionedChunks(const Chunk & chunk, std::optional<ChunkPartitioner> & chunk_partitioner)
{
    if (chunk_partitioner.has_value())
        return chunk_partitioner->partitionChunk(chunk);
    auto unpartitioned_result = std::vector<std::pair<ChunkPartitioner::PartitionKey, Chunk>>{};
    unpartitioned_result.emplace_back(ChunkPartitioner::PartitionKey{}, chunk.clone());
    return unpartitioned_result;
}


static std::optional<WriteDataFilesResult> writeDataFiles(
    const MutationCommands & commands,
    ContextPtr context,
    StorageMetadataPtr metadata,
    StorageID storage_id,
    ObjectStoragePtr object_storage,
    String write_format,
    FileNamesGenerator & generator,
    const Iceberg::IcebergPathResolver & path_resolver,
    const std::optional<FormatSettings> & format_settings,
    std::optional<ChunkPartitioner> & chunk_partitioner,
    Poco::JSON::Object::Ptr data_schema)
{
    chassert(commands.size() == 1);

    auto storage_ptr = DatabaseCatalog::instance().getTable(storage_id, context);
    DataFileWriteResultByPartitionKey delete_data_result;
    DataFileStatisticsByPartitionKey delete_data_statistics;
    std::unordered_map<ChunkPartitioner::PartitionKey, std::unique_ptr<WriteBuffer>, ChunkPartitioner::PartitionKeyHasher> delete_data_write_buffers;
    std::unordered_map<ChunkPartitioner::PartitionKey, OutputFormatPtr, ChunkPartitioner::PartitionKeyHasher> delete_data_writers;

    DataFileWriteResultByPartitionKey update_data_result;
    DataFileStatisticsByPartitionKey update_data_statistics;
    std::unordered_map<ChunkPartitioner::PartitionKey, std::unique_ptr<WriteBuffer>, ChunkPartitioner::PartitionKeyHasher> update_data_write_buffers;
    std::unordered_map<ChunkPartitioner::PartitionKey, OutputFormatPtr, ChunkPartitioner::PartitionKeyHasher> update_data_writers;

    if (commands[0].type == MutationCommand::UPDATE || commands[0].type == MutationCommand::DELETE)
    {
        MutationsInterpreter::Settings settings(true);
        settings.return_all_columns = true;
        settings.return_mutated_rows = true;

        auto delete_commands = commands;
        delete_commands[0].type = MutationCommand::DELETE;

        auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata, delete_commands, context, settings);
        auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
        PullingPipelineExecutor executor(pipeline);

        auto header = interpreter->getUpdatedHeader();

        Block block;
        bool has_any_rows = false;
        while (executor.pull(block))
        {
            if (block.rows() == 0)
                continue;

            has_any_rows = true;
            Chunk chunk(block.getColumns(), block.rows());
            auto partition_result = getPartitionedChunks(chunk, chunk_partitioner);


            size_t col_data_filename_index = block.getPositionByName(block_datafile_path);
            size_t col_position_index = block.getPositionByName(block_row_number);
            ColumnWithTypeAndName col_data_filename = block.getByPosition(col_data_filename_index);
            ColumnWithTypeAndName col_position = block.getByPosition(col_position_index);

            for (const auto & [partition_key, partition_chunk] : partition_result)
            {
                if (!delete_data_statistics.contains(partition_key))
                    delete_data_statistics.emplace(partition_key, DataFileStatistics(IcebergPositionDeleteTransform::getSchemaFields()));

                if (!delete_data_writers.contains(partition_key))
                {
                    auto delete_file_path = generator.generatePositionDeleteFile();

                    delete_data_result[partition_key].path = delete_file_path;
                    auto write_buffer = object_storage->writeObject(
                        StoredObject(path_resolver.resolve(delete_file_path)),
                        WriteMode::Rewrite,
                        std::nullopt,
                        DBMS_DEFAULT_BUFFER_SIZE,
                        context->getWriteSettings());

                    auto delete_file_sample_block = getPositionDeleteFileSampleBlock();
                    ColumnMapperPtr column_mapper = std::make_shared<ColumnMapper>();
                    std::unordered_map<String, Int64> field_ids;
                    field_ids[IcebergPositionDeleteTransform::positions_column_name] = IcebergPositionDeleteTransform::positions_column_field_id;
                    field_ids[IcebergPositionDeleteTransform::data_file_path_column_name] = IcebergPositionDeleteTransform::data_file_path_column_field_id;
                    column_mapper->setStorageColumnEncoding(std::move(field_ids));
                    FormatFilterInfoPtr format_filter_info = std::make_shared<FormatFilterInfo>(nullptr, context, column_mapper, nullptr, nullptr);
                    auto output_format = FormatFactory::instance().getOutputFormat(
                        write_format, *write_buffer, delete_file_sample_block, context, format_settings, format_filter_info);

                    delete_data_write_buffers[partition_key] = std::move(write_buffer);
                    delete_data_writers[partition_key] = std::move(output_format);
                }

                col_data_filename.column = partition_chunk.getColumns()[col_data_filename_index];
                col_position.column = partition_chunk.getColumns()[col_position_index];

                /// The virtual column `_iceberg_metadata_file_path` may arrive as
                /// LowCardinality(String) from the pipeline, but the position delete
                /// file format expects plain String. Unwrap it.
                col_data_filename.column = col_data_filename.column->convertToFullColumnIfLowCardinality();
                col_data_filename.type = removeLowCardinality(col_data_filename.type);

                if (const ColumnNullable * nullable = typeid_cast<const ColumnNullable *>(col_position.column.get()))
                {
                    const auto & null_map = nullable->getNullMapData();
                    if (std::any_of(null_map.begin(), null_map.end(), [](UInt8 x) { return x != 0; }))
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected null _row_number");
                    col_position.column = nullable->getNestedColumnPtr();
                    col_position.type = removeNullable(col_position.type);
                }

                /// _iceberg_metadata_file_path already contains the correct metadata path format
                /// (e.g. wasb://container@host/.../data/xxx.parquet or /iceberg/.../data/xxx.parquet)
                /// so no transformation is needed.
                Columns chunk_pos_delete;
                chunk_pos_delete.push_back(col_data_filename.column);
                chunk_pos_delete.push_back(col_position.column);
                auto stats_chunk = Chunk(chunk_pos_delete, partition_chunk.getNumRows());
                delete_data_statistics.at(partition_key).update(stats_chunk);

                Block delete_file_block({col_data_filename, col_position});
                delete_data_result[partition_key].total_rows += delete_file_block.rows();
                delete_data_writers[partition_key]->write(delete_file_block);
            }
        }

        if (!has_any_rows)
            return std::nullopt;

        for (const auto & [partition_key, _] : delete_data_result)
        {
            delete_data_writers[partition_key]->flush();
            delete_data_writers[partition_key]->finalize();
            delete_data_write_buffers[partition_key]->finalize();
            {
                auto delete_bytes = delete_data_write_buffers[partition_key]->count();
                if (delete_bytes == 0)
                    delete_bytes = object_storage->getObjectMetadata(
                        path_resolver.resolve(delete_data_result[partition_key].path), /*with_tags=*/ false).size_bytes;
                delete_data_result[partition_key].total_bytes = static_cast<Int32>(delete_bytes);
            }
        }
    }

    if (commands[0].type == MutationCommand::UPDATE)
    {
        MutationsInterpreter::Settings settings(true);
        settings.return_all_columns = true;
        settings.return_mutated_rows = true;

        auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata, commands, context, settings);
        auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
        PullingPipelineExecutor executor(pipeline);

        auto header = interpreter->getUpdatedHeader();

        Block block;
        while (executor.pull(block))
        {
            if (block.rows() == 0)
                continue;

            /// Strip virtual columns and unwrap LowCardinality to ensure the
            /// block types are compatible with the Avro serializer schema.
            auto data_block = getNonVirtualColumns(block, /*remove_low_cardinality=*/ true);
            Chunk chunk(data_block.getColumns(), data_block.rows());
            auto partition_result = getPartitionedChunks(chunk, chunk_partitioner);

            for (const auto & [partition_key, partition_chunk] : partition_result)
            {
                if (!update_data_statistics.contains(partition_key))
                    update_data_statistics.emplace(partition_key, DataFileStatistics(data_schema->getArray(Iceberg::f_fields)));

                auto it = update_data_writers.find(partition_key);
                if (it == update_data_writers.end())
                {
                    auto data_file_path = generator.generateDataFileName();
                    update_data_result[partition_key].path = data_file_path;
                    auto data_write_buffer = object_storage->writeObject(
                        StoredObject(path_resolver.resolve(data_file_path)),
                        WriteMode::Rewrite,
                        std::nullopt,
                        DBMS_DEFAULT_BUFFER_SIZE,
                        context->getWriteSettings());

                    ColumnMapperPtr data_column_mapper = createColumnMapper(data_schema);
                    FormatFilterInfoPtr data_format_filter_info = std::make_shared<FormatFilterInfo>(nullptr, context, data_column_mapper, nullptr, nullptr);
                    auto data_output_format = FormatFactory::instance().getOutputFormat(
                        write_format, *data_write_buffer, data_block, context, format_settings, data_format_filter_info);

                    update_data_write_buffers[partition_key] = std::move(data_write_buffer);
                    it = update_data_writers.emplace(partition_key, std::move(data_output_format)).first;
                }

                update_data_result[partition_key].total_rows += data_block.rows();
                it->second->write(data_block);
                update_data_statistics.at(partition_key).update(chunk);
            }
        }

        for (const auto & [partition_key, _] : update_data_result)
        {
            update_data_writers[partition_key]->flush();
            update_data_writers[partition_key]->finalize();
            update_data_write_buffers[partition_key]->finalize();
            {
                auto update_bytes = update_data_write_buffers[partition_key]->count();
                if (update_bytes == 0)
                    update_bytes = object_storage->getObjectMetadata(
                        path_resolver.resolve(update_data_result[partition_key].path), /*with_tags=*/ false).size_bytes;
                update_data_result[partition_key].total_bytes = static_cast<Int32>(update_bytes);
            }
        }
    }

    if (commands[0].type == MutationCommand::DELETE)
        return WriteDataFilesResult{DataFileWriteResultWithStats{delete_data_result, delete_data_statistics}, std::nullopt};
    else if (commands[0].type == MutationCommand::UPDATE)
        return WriteDataFilesResult{DataFileWriteResultWithStats{delete_data_result, delete_data_statistics}, DataFileWriteResultWithStats{update_data_result, update_data_statistics}};
    else
        return {};
}

static bool writeMetadataFiles(
    DataFileWriteResultWithStats & delete_filenames,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    FileNamesGenerator & filename_generator,
    const Iceberg::IcebergPathResolver & path_resolver,
    const DataLakeStorageSettings & data_lake_settings,
    String write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    StorageID table_id,
    Poco::JSON::Object::Ptr metadata,
    Poco::JSON::Object::Ptr partititon_spec,
    Int32 partition_spec_id,
    std::optional<ChunkPartitioner> & chunk_partitioner,
    Iceberg::FileContentType content_type,
    SharedHeader sample_block,
    bool write_metadata_json_file)
{
    auto metadata_info = filename_generator.generateMetadataPathWithInfo();
    auto storage_metadata_name = path_resolver.resolve(metadata_info.path);
    Int64 parent_snapshot = -1;
    if (metadata->has(Iceberg::f_current_snapshot_id))
        parent_snapshot = metadata->getValue<Int64>(Iceberg::f_current_snapshot_id);

    Int64 total_rows = 0;
    Int64 total_bytes = 0;
    Int64 total_files = 0;
    for (const auto & [_, delete_filename] : delete_filenames.delete_file)
    {
        total_rows += delete_filename.total_rows;
        total_bytes += delete_filename.total_bytes;
        ++total_files;
    }

    Poco::JSON::Object::Ptr new_snapshot;
    String storage_manifest_list_name;
    if (content_type == Iceberg::FileContentType::POSITION_DELETE)
    {
        auto result = MetadataGenerator(metadata).generateNextMetadata(
            filename_generator,
            metadata_info.path,
            parent_snapshot,
            /* added_files */ 0,
            /* added_records */ 0,
            total_bytes,
            /* num_partitions */ total_files,
            /* added_delete_files */ total_files,
            total_rows);
        new_snapshot = result.snapshot;
        storage_manifest_list_name = path_resolver.resolve(result.manifest_list_path);
    }
    else
    {
        auto result = MetadataGenerator(metadata).generateNextMetadata(
            filename_generator,
            metadata_info.path,
            parent_snapshot,
            /* added_files */ total_files,
            /* added_records */ total_rows,
            total_bytes,
            /* num_partitions */ total_files,
            /* added_delete_files */ 0,
            /*num_deleted_rows*/ 0);
        new_snapshot = result.snapshot;
        storage_manifest_list_name = path_resolver.resolve(result.manifest_list_path);
    }
    auto manifest_entries_in_storage = std::make_shared<Strings>();
    std::vector<Iceberg::IcebergPathFromMetadata> manifest_entries;
    std::vector<Int64> manifest_entry_sizes;

    auto cleanup = [object_storage, &delete_filenames, &path_resolver, manifest_entries_in_storage, storage_manifest_list_name, storage_metadata_name]()
    {
        try
        {
            for (const auto & [_, data_file] : delete_filenames.delete_file)
                object_storage->removeObjectIfExists(StoredObject(path_resolver.resolve(data_file.path)));

            for (const auto & manifest_filename_in_storage : *manifest_entries_in_storage)
                object_storage->removeObjectIfExists(StoredObject(manifest_filename_in_storage));

            object_storage->removeObjectIfExists(StoredObject(storage_manifest_list_name));
        }
        catch (...)
        {
            LOG_DEBUG(getLogger("IcebergMutations"), "Iceberg cleanup failed");
        }
    };

    try
    {
        for (const auto & [partition_key, delete_filename] : delete_filenames.delete_file)
        {
            auto manifest_entry_path = filename_generator.generateManifestEntryName();
            manifest_entries_in_storage->push_back(path_resolver.resolve(manifest_entry_path));
            manifest_entries.push_back(manifest_entry_path);

            auto buffer_manifest_entry = object_storage->writeObject(
                StoredObject(path_resolver.resolve(manifest_entry_path)),
                WriteMode::Rewrite,
                std::nullopt,
                DBMS_DEFAULT_BUFFER_SIZE,
                context->getWriteSettings());
            try
            {
                generateManifestFile(
                    metadata,
                    chunk_partitioner ? chunk_partitioner->getColumns() : std::vector<String>{},
                    partition_key,
                    chunk_partitioner ? chunk_partitioner->getResultTypes() : std::vector<DataTypePtr>{},
                    {delete_filename.path},
                    {static_cast<UInt64>(delete_filename.total_rows)},
                    {static_cast<UInt64>(delete_filename.total_bytes)},
                    delete_filenames.delete_statistic.at(partition_key),
                    sample_block,
                    new_snapshot,
                    write_format,
                    partititon_spec,
                    partition_spec_id,
                    *buffer_manifest_entry,
                    content_type);
                buffer_manifest_entry->finalize();
                auto size = buffer_manifest_entry->count();
                if (size == 0)
                {
                    size = object_storage->getObjectMetadata(path_resolver.resolve(manifest_entry_path), /*with_tags=*/false).size_bytes;
                }
                manifest_entry_sizes.push_back(size);
            }
            catch (...)
            {
                cleanup();
                throw;
            }
        }

        {
            auto buffer_manifest_list = object_storage->writeObject(
                StoredObject(storage_manifest_list_name),
                WriteMode::Rewrite,
                std::nullopt,
                DBMS_DEFAULT_BUFFER_SIZE,
                context->getWriteSettings());

            try
            {
                generateManifestList(
                    path_resolver,
                    metadata,
                    object_storage,
                    context,
                    manifest_entries,
                    new_snapshot,
                    manifest_entry_sizes,
                    *buffer_manifest_list,
                    content_type);
                buffer_manifest_list->finalize();
            }
            catch (...)
            {
                cleanup();
                throw;
            }
        }

        if (write_metadata_json_file)
        {
            std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            Poco::JSON::Stringifier::stringify(metadata, oss, 4);
            std::string json_representation = removeEscapedSlashes(oss.str());

            fiu_do_on(FailPoints::iceberg_writes_cleanup,
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failpoint for cleanup enabled");
            });

            auto hint_path = filename_generator.generateVersionHint();
            if (!writeMetadataFileAndVersionHint(
                    path_resolver,
                    metadata_info,
                    json_representation,
                    hint_path,
                    object_storage,
                    context,
                    data_lake_settings[DataLakeStorageSetting::iceberg_use_version_hint]))
            {
                cleanup();
                return false;
            }

            if (catalog)
            {
                auto catalog_filename = path_resolver.resolveForCatalog(metadata_info.path);
                const auto & [namespace_name, table_name] = DataLake::parseTableName(table_id.getTableName());
                if (!catalog->updateMetadata(namespace_name, table_name, catalog_filename, new_snapshot))
                {
                    cleanup();
                    return false;
                }
            }
        }
    }
    catch (...)
    {
        cleanup();
        throw;
    }
    return true;
}

void mutate(
    const MutationCommands & commands,
    ContextPtr context,
    StorageMetadataPtr storage_metadata,
    StorageID storage_id,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_table_components,
    const String & write_format,
    const std::optional<FormatSettings> & format_settings,
    std::shared_ptr<DataLake::ICatalog> catalog)
{
    auto common_path = persistent_table_components.table_path;
    if (!common_path.starts_with('/'))
        common_path = "/" + common_path;

    int max_retries = MAX_TRANSACTION_RETRIES;
    while (--max_retries > 0)
    {
        auto log = getLogger("IcebergMutations");
        /// Mutations must always operate on the actual latest metadata, regardless of
        /// any explicit iceberg_metadata_file_path set on the table (used for time-travel reads).
        auto [last_version, metadata_path, compression_method] = getLatestOrExplicitMetadataFileAndVersion(
            object_storage,
            persistent_table_components.table_path,
            data_lake_settings,
            persistent_table_components.metadata_cache,
            context,
            log.get(),
            persistent_table_components.table_uuid,
            persistent_table_components.metadata_compression_method,
            /* force_fetch_latest_metadata */ true,
            /* ignore_explicit_metadata_file_path */ true);

        FileNamesGenerator filename_generator(persistent_table_components.path_resolver.getTableLocation(), false, CompressionMethod::None, write_format);
        filename_generator.setVersion(last_version + 1);
        filename_generator.setCompressionMethod(compression_method);

        auto metadata = getMetadataJSONObject(metadata_path, object_storage, persistent_table_components.metadata_cache, context, log, compression_method, persistent_table_components.table_uuid);

        if (metadata->getValue<Int32>(f_format_version) < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mutations are supported only for the second version of iceberg format");
        auto partition_spec_id = metadata->getValue<Int64>(Iceberg::f_default_spec_id);
        auto partitions_specs = metadata->getArray(Iceberg::f_partition_specs);
        Poco::JSON::Object::Ptr partititon_spec;
        for (size_t i = 0; i < partitions_specs->size(); ++i)
        {
            auto current_partition_spec = partitions_specs->getObject(static_cast<UInt32>(i));
            if (current_partition_spec->getValue<Int64>(Iceberg::f_spec_id) == partition_spec_id)
            {
                partititon_spec = current_partition_spec;
                break;
            }
        }

        auto current_schema_id = metadata->getValue<Int64>(Iceberg::f_current_schema_id);
        Poco::JSON::Object::Ptr current_schema;
        auto schemas = metadata->getArray(Iceberg::f_schemas);
        for (size_t i = 0; i < schemas->size(); ++i)
        {
            if (schemas->getObject(static_cast<UInt32>(i))->getValue<Int32>(Iceberg::f_schema_id) == current_schema_id)
            {
                current_schema = schemas->getObject(static_cast<UInt32>(i));
            }
        }

        TableStateSnapshot current_iceberg_snapshot;
        current_iceberg_snapshot.metadata_file_path = metadata_path;
        current_iceberg_snapshot.metadata_version = last_version;
        current_iceberg_snapshot.schema_id = static_cast<Int32>(current_schema_id);
        if (metadata->has(Iceberg::f_current_snapshot_id))
        {
            Int64 snapshot_id_val = metadata->getValue<Int64>(Iceberg::f_current_snapshot_id);
            if (snapshot_id_val >= 0)
                current_iceberg_snapshot.snapshot_id = snapshot_id_val;
        }
        auto fresh_storage_metadata = std::make_shared<StorageInMemoryMetadata>(*storage_metadata);
        fresh_storage_metadata->setDataLakeTableState(DataLakeTableStateSnapshot{current_iceberg_snapshot});

        const auto sample_block = std::make_shared<const Block>(fresh_storage_metadata->getSampleBlock());
        std::optional<ChunkPartitioner> chunk_partitioner;
        if (partititon_spec->has(Iceberg::f_fields) && partititon_spec->getArray(Iceberg::f_fields)->size() > 0)
            chunk_partitioner = ChunkPartitioner(partititon_spec->getArray(Iceberg::f_fields), current_schema->getArray(Iceberg::f_fields), context, sample_block);

        auto mutation_files = writeDataFiles(
            commands,
            context,
            fresh_storage_metadata,
            storage_id,
            object_storage,
            write_format,
            filename_generator,
            persistent_table_components.path_resolver,
            format_settings,
            chunk_partitioner,
            current_schema);

        if (mutation_files)
        {
            auto result_delete_files_metadata = writeMetadataFiles(
                mutation_files->delete_file,
                object_storage,
                context,
                filename_generator,
                persistent_table_components.path_resolver,
                data_lake_settings,
                write_format,
                catalog,
                storage_id,
                metadata,
                partititon_spec,
                static_cast<Int32>(partition_spec_id),
                chunk_partitioner,
                Iceberg::FileContentType::POSITION_DELETE,
                std::make_shared<const Block>(getPositionDeleteFileSampleBlock()),
                !mutation_files->data_file);
            if (!result_delete_files_metadata)
                continue;

            if (mutation_files->data_file)
            {
                auto result_data_files_metadata = writeMetadataFiles(
                    *mutation_files->data_file,
                    object_storage,
                    context,
                    filename_generator,
                    persistent_table_components.path_resolver,
                    data_lake_settings,
                    write_format,
                    catalog,
                    storage_id,
                    metadata,
                    partititon_spec,
                    static_cast<Int32>(partition_spec_id),
                    chunk_partitioner,
                    Iceberg::FileContentType::DATA,
                    sample_block,
                    true);
                if (!result_data_files_metadata)
                {
                    continue;
                }
            }
        }
        break;
    }

    if (max_retries == 0)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Too many unsuccessed retries to create iceberg snapshot");
}

void alter(
    const AlterCommands & params,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_table_components,
    const String & write_format)
{
    if (params.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Params with size 1 is not supported");

    size_t i = 0;
    bool succeeded = false;
    while (i < MAX_TRANSACTION_RETRIES)
    {
        auto log = getLogger("IcebergMutations");
        auto [last_version, metadata_path, compression_method] = getLatestOrExplicitMetadataFileAndVersion(
            object_storage,
            persistent_table_components.table_path,
            data_lake_settings,
            persistent_table_components.metadata_cache,
            context,
            log.get(),
            persistent_table_components.table_uuid,
            persistent_table_components.metadata_compression_method,
            /* force_fetch_latest_metadata */ true,
            /* ignore_explicit_metadata_file_path */ true);

        FileNamesGenerator filename_generator(persistent_table_components.path_resolver.getTableLocation(), false, CompressionMethod::None, write_format);
        filename_generator.setVersion(last_version + 1);
        filename_generator.setCompressionMethod(compression_method);

        auto metadata = getMetadataJSONObject(metadata_path, object_storage, persistent_table_components.metadata_cache, context, log, compression_method, persistent_table_components.table_uuid);

        auto metadata_json_generator = MetadataGenerator(metadata);

        switch (params[0].type)
        {
            case AlterCommand::Type::ADD_COLUMN:
                metadata_json_generator.generateAddColumnMetadata(params[0].column_name, params[0].data_type);
                break;
            case AlterCommand::Type::DROP_COLUMN:
                if (params[0].clear)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Clear column is not supported for iceberg. Please use UPDATE instead");
                metadata_json_generator.generateDropColumnMetadata(params[0].column_name);
                break;
            case AlterCommand::Type::MODIFY_COLUMN:
                metadata_json_generator.generateModifyColumnMetadata(params[0].column_name, params[0].data_type);
                break;
            case AlterCommand::Type::RENAME_COLUMN:
                metadata_json_generator.generateRenameColumnMetadata(params[0].column_name, params[0].rename_to);
                break;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown type of alter {}", params[0].type);
        }

        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::JSON::Stringifier::stringify(metadata, oss, 4);
        std::string json_representation = removeEscapedSlashes(oss.str());

        auto metadata_info = filename_generator.generateMetadataPathWithInfo();

        auto hint_path = filename_generator.generateVersionHint();
        if (writeMetadataFileAndVersionHint(
                persistent_table_components.path_resolver,
                metadata_info,
                json_representation,
                hint_path,
                object_storage,
                context,
                data_lake_settings[DataLakeStorageSetting::iceberg_use_version_hint]))
        {
            succeeded = true;
            break;
        }
        ++i;
    }

    if (!succeeded)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Too many unsuccessed retries to alter iceberg table");

    /// Invalidate the metadata files cache so that subsequent operations on this table see the
    /// schema we just wrote. See `PersistentTableComponents::invalidateMetadataCache` for the
    /// rationale.
    persistent_table_components.invalidateMetadataCache();
}

#endif

}

namespace DB
{

Pipe IcebergMetadata::alterPartition(const PartitionCommands & commands, ContextPtr context)
{
    if (!context->getSettingsRef()[Setting::allow_insert_into_iceberg].value)
    {
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Alter iceberg is experimental. "
            "To allow its usage, enable setting allow_insert_into_iceberg");
    }
#if USE_AVRO
    if (commands.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Params with size 1 is not supported");

    const auto & command = commands.at(0);

    switch (command.type)
    {
        case PartitionCommand::Type::DROP_PARTITION: {
            if (command.part || command.detach)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not supported of Iceberg", command.typeToString());

            alterPartitionDropImpl(command, context);
            break;
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not supported of Iceberg", command.typeToString());
    }
#else
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter partition is not supported of Iceberg", command.typeToString());
#endif
    return {};
}

#if USE_AVRO
namespace
{

/// Translate the natural type of a Field stored in a parsed manifest's partition tuple
/// into a ClickHouse DataType, used to build the Avro schema of the rewritten manifest
/// file. The mapping mirrors what the Avro reader produced when parsing the original
/// manifest, so the encoded values round-trip safely.
DataTypePtr inferPartitionType(const Field & f)
{
    switch (f.getType())
    {
        case Field::Types::Int64:
        case Field::Types::UInt64:
            return std::make_shared<DataTypeInt64>();
        case Field::Types::String:
            return std::make_shared<DataTypeString>();
        case Field::Types::Float64:
            return std::make_shared<DataTypeFloat64>();
        case Field::Types::Decimal32:
            return std::make_shared<DataTypeDecimal<Decimal32>>(9, 0);
        case Field::Types::Decimal64:
            return std::make_shared<DataTypeDecimal<Decimal64>>(18, 0);
        case Field::Types::Null:
            return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        default:
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Unsupported partition value type {} when rewriting manifest for DROP PARTITION",
                f.getType());
    }
}

bool partitionEquals(const DB::Row & lhs, const DB::Row & rhs)
{
    if (lhs.size() != rhs.size())
        return false;
    for (size_t i = 0; i < lhs.size(); ++i)
        if (!accurateEquals(lhs[i], rhs[i]))
            return false;
    return true;
}

/// Parse the partition value from an ASTPartition into a tuple of Field values matching
/// the partition spec's arity. Accepts scalar literal (arity must be 1), tuple literal,
/// or a `tuple(...)` function call of literals. User-supplied values are taken as the
/// already-transformed partition values (e.g. integer day-since-epoch for `day(ts)`).
DB::Row parsePartitionTuple(const IAST * value_ast, size_t arity)
{
    DB::Row out;
    out.reserve(arity);

    if (const auto * lit = value_ast->as<ASTLiteral>())
    {
        if (lit->value.getType() == Field::Types::Tuple)
        {
            const auto & t = lit->value.safeGet<Tuple>();
            if (t.size() != arity)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "DROP PARTITION value has {} fields but partition spec has {}", t.size(), arity);
            for (const auto & f : t)
                out.push_back(f);
            return out;
        }
        if (arity != 1)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "DROP PARTITION expects a tuple of {} values for this table, got a scalar", arity);
        out.push_back(lit->value);
        return out;
    }

    if (const auto * fn = value_ast->as<ASTFunction>(); fn && fn->name == "tuple")
    {
        const auto & args = fn->arguments ? fn->arguments->children : ASTs{};
        if (args.size() != arity)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "DROP PARTITION value has {} fields but partition spec has {}", args.size(), arity);
        for (const auto & arg : args)
        {
            const auto * arg_lit = arg->as<ASTLiteral>();
            if (!arg_lit)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "DROP PARTITION supports only literal partition values for Iceberg");
            out.push_back(arg_lit->value);
        }
        return out;
    }

    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "DROP PARTITION supports only literal partition values for Iceberg");
}

}
#endif

void IcebergMetadata::alterPartitionDropImpl(const PartitionCommand & command, ContextPtr context)
{
#if USE_AVRO
    /// Algorithm (matches the design notes that were on this function originally):
    ///   1. Load latest metadata & current snapshot.
    ///   2. For each manifest in the snapshot, classify entries as either matching the
    ///      requested partition (to remove) or surviving.
    ///   3. Rewrite manifests:
    ///        - all-match  manifest  → drop it from the new manifest list
    ///        - partial    manifest  → write a replacement manifest with surviving
    ///                                 entries re-emitted with status=EXISTING (their
    ///                                 original snapshot_id and sequence_number are
    ///                                 preserved per Iceberg v2 spec)
    ///        - no-match   manifest  → carry over verbatim from the parent's list
    ///   4. Build a new "delete" snapshot summarising removed counts, then write a new
    ///      metadata.json (CAS) committing the result.
    ///   5. On commit conflict (concurrent writer), re-fetch the latest snapshot and
    ///      retry. Across retries we keep targeting the same set of data-file paths
    ///      identified on the first pass — compaction may have moved them between
    ///      manifests, but the *paths* are stable; this means a concurrent insert
    ///      into a different partition cannot make us fail, and a concurrent insert
    ///      into the same partition lands AFTER our drop, leaving its rows intact.
    const auto & partition_ast = command.partition->as<ASTPartition>();

    if (partition_ast->all)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} ALL is not supported for Iceberg", command.typeToString());

    if (partition_ast->id)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} ID is not supported for Iceberg", command.typeToString());

    if (!partition_ast->value)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} doesn't have partition value", command.typeToString());

    auto drop_log = getLogger("IcebergPartitionDrop");

    /// Set of storage-resolved data-file paths to drop, locked in on the first attempt.
    std::unordered_set<String> data_paths_to_remove;
    std::unordered_set<String> position_delete_paths_to_remove;
    bool target_set_initialized = false;

    int attempts = 0;
    while (attempts++ < Iceberg::MAX_TRANSACTION_RETRIES)
    {
        auto [data_snapshot, table_state] = getRelevantState(context, /*force_fetch_latest_metadata=*/true);
        if (!data_snapshot)
        {
            LOG_DEBUG(drop_log, "Table has no snapshot, nothing to drop");
            return;
        }

        auto metadata_object = Iceberg::getMetadataJSONObject(
            table_state.metadata_file_path,
            object_storage,
            persistent_components.metadata_cache,
            context,
            log,
            persistent_components.metadata_compression_method,
            persistent_components.table_uuid);

        if (metadata_object->getValue<Int32>(Iceberg::f_format_version) < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "DROP PARTITION is supported only for Iceberg format-version 2");

        /// Locate the current default partition spec.
        auto partition_spec_id = metadata_object->getValue<Int64>(Iceberg::f_default_spec_id);
        Poco::JSON::Object::Ptr partition_spec;
        {
            auto specs = metadata_object->getArray(Iceberg::f_partition_specs);
            for (size_t i = 0; i < specs->size(); ++i)
            {
                auto p = specs->getObject(static_cast<UInt32>(i));
                if (p->getValue<Int64>(Iceberg::f_spec_id) == partition_spec_id)
                {
                    partition_spec = p;
                    break;
                }
            }
        }
        if (!partition_spec)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Default partition spec {} not found in metadata", partition_spec_id);

        auto partition_fields = partition_spec->getArray(Iceberg::f_fields);
        const size_t partition_arity = partition_fields->size();
        if (partition_arity == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "DROP PARTITION is not supported on unpartitioned Iceberg tables");

        std::vector<String> partition_columns;
        partition_columns.reserve(partition_arity);
        for (size_t i = 0; i < partition_arity; ++i)
            partition_columns.push_back(partition_fields->getObject(static_cast<UInt32>(i))->getValue<String>(Iceberg::f_partition_name));

        const DB::Row target_partition = parsePartitionTuple(partition_ast->value, partition_arity);

        /// Iterate manifests for the current snapshot and group entries per manifest so
        /// we can classify each manifest as drop / rewrite / keep.
        struct ManifestPlan
        {
            Iceberg::IcebergPathFromMetadata path;
            Int64 byte_size = 0;
            Iceberg::ManifestFileContentType manifest_content_type = Iceberg::ManifestFileContentType::DATA;
            std::vector<Iceberg::ProcessedManifestFileEntryPtr> survivors;   // status != DELETED, not matched
            std::vector<Iceberg::ProcessedManifestFileEntryPtr> matched;     // to be removed
        };

        std::vector<ManifestPlan> manifest_plans;
        manifest_plans.reserve(data_snapshot->manifest_list_entries.size());

        Int32 schema_id = data_snapshot->schema_id_on_snapshot_commit;

        for (const auto & manifest_key : data_snapshot->manifest_list_entries)
        {
            ManifestPlan plan;
            plan.path = manifest_key.manifest_file_path;
            plan.byte_size = static_cast<Int64>(manifest_key.manifest_file_byte_size);
            plan.manifest_content_type = manifest_key.content_type;

            auto handle = Iceberg::getManifestFileEntriesHandle(
                object_storage, persistent_components, context, log, manifest_key, schema_id);

            auto classify = [&](const std::vector<Iceberg::ProcessedManifestFileEntryPtr> & entries,
                                std::unordered_set<String> * tracking_set,
                                Iceberg::FileContentType content_type)
            {
                for (const auto & entry : entries)
                {
                    const auto & parsed = entry->parsed_entry;
                    if (!parsed)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Manifest file entry is not parsed");

                    bool matches = false;
                    if (target_set_initialized)
                    {
                        const String storage_path = persistent_components.path_resolver.resolve(parsed->file_path_key);
                        const auto & target = (content_type == Iceberg::FileContentType::DATA)
                            ? data_paths_to_remove
                            : position_delete_paths_to_remove;
                        matches = target.contains(storage_path);
                    }
                    else
                    {
                        matches = partitionEquals(parsed->partition_key_value, target_partition);
                        if (matches && tracking_set)
                        {
                            tracking_set->insert(persistent_components.path_resolver.resolve(parsed->file_path_key));
                        }
                    }

                    if (matches)
                        plan.matched.push_back(entry);
                    else
                        plan.survivors.push_back(entry);
                }
            };

            classify(handle.getFilesWithoutDeleted(Iceberg::FileContentType::DATA),
                     target_set_initialized ? nullptr : &data_paths_to_remove,
                     Iceberg::FileContentType::DATA);
            classify(handle.getFilesWithoutDeleted(Iceberg::FileContentType::POSITION_DELETE),
                     target_set_initialized ? nullptr : &position_delete_paths_to_remove,
                     Iceberg::FileContentType::POSITION_DELETE);

            manifest_plans.push_back(std::move(plan));
        }

        target_set_initialized = true;

        if (data_paths_to_remove.empty() && position_delete_paths_to_remove.empty())
        {
            LOG_INFO(log, "No data files match the requested partition; DROP PARTITION is a no-op");
            return;
        }

        /// Tally removed-file statistics and split manifests into drop / rewrite / keep.
        Int64 removed_data_files = 0;
        Int64 removed_records = 0;
        Int64 removed_files_size = 0;
        Int64 removed_position_delete_files = 0;
        Int64 removed_position_deletes = 0;
        std::set<DB::Row> changed_partitions;

        std::unordered_set<String> skip_manifest_paths;
        struct RewriteOutput
        {
            Iceberg::IcebergPathFromMetadata new_manifest_path;
            Int64 manifest_length = 0;
            Int32 existing_files_count = 0;
            Int32 existing_rows_count = 0;
            /// Smallest sequence number among the surviving entries; goes into the
            /// manifest list as `min_sequence_number`. The manifest list's own
            /// `added_snapshot_id` / `sequence_number` are populated by the writer
            /// from the new (DROP) snapshot — the rewritten manifest *file* is new.
            Int64 min_sequence_number = 0;
            Iceberg::FileContentType content_type = Iceberg::FileContentType::DATA;
        };
        std::vector<RewriteOutput> rewrites;

        FileNamesGenerator filename_generator(
            persistent_components.path_resolver.getTableLocation(),
            false,
            persistent_components.metadata_compression_method,
            write_format);
        filename_generator.setVersion(table_state.metadata_version + 1);
        filename_generator.setCompressionMethod(persistent_components.metadata_compression_method);

        /// Derive partition types from the first matched entry's tuple. This is sufficient
        /// for re-emitting Avro records: the values being written are exactly the Fields
        /// we already parsed out of the original manifests, so type round-trip is exact.
        std::vector<DataTypePtr> partition_types;
        partition_types.reserve(partition_arity);
        for (size_t i = 0; i < partition_arity; ++i)
        {
            DataTypePtr t;
            for (const auto & plan : manifest_plans)
            {
                auto pick = [&](const std::vector<Iceberg::ProcessedManifestFileEntryPtr> & v) -> DataTypePtr
                {
                    for (const auto & e : v)
                        if (i < e->parsed_entry->partition_key_value.size())
                            return inferPartitionType(e->parsed_entry->partition_key_value[i]);
                    return nullptr;
                };
                t = pick(plan.matched);
                if (!t) t = pick(plan.survivors);
                if (t) break;
            }
            if (!t)
                t = inferPartitionType(target_partition[i]);
            partition_types.push_back(t);
        }

        /// Cleanup of any new manifest files we wrote, used if commit fails or throws.
        std::vector<String> wrote_for_cleanup;
        auto cleanup = [&]()
        {
            for (const auto & p : wrote_for_cleanup)
            {
                try { object_storage->removeObjectIfExists(StoredObject(p)); }
                catch (...) { tryLogCurrentException(log, "Failed to clean up partially-written manifest"); }
            }
        };

        try
        {
            for (auto & plan : manifest_plans)
            {
                if (plan.matched.empty())
                    continue;

                for (const auto & m : plan.matched)
                {
                    const auto & p = *m->parsed_entry;
                    if (p.content_type == Iceberg::FileContentType::DATA)
                    {
                        ++removed_data_files;
                        removed_records += p.record_count;
                        removed_files_size += p.file_size_in_bytes;
                    }
                    else if (p.content_type == Iceberg::FileContentType::POSITION_DELETE)
                    {
                        ++removed_position_delete_files;
                        removed_position_deletes += p.record_count;
                    }
                    changed_partitions.insert(p.partition_key_value);
                }

                if (plan.survivors.empty())
                {
                    /// All entries match — drop the whole manifest from the new list.
                    skip_manifest_paths.insert(plan.path.serialize());
                    continue;
                }

                /// Partial match — write a replacement manifest with surviving entries
                /// as status=EXISTING. Skip if there is mixed content_type (would need
                /// two replacement manifests). In practice data and position-delete
                /// manifests are separate per spec, so survivors are homogeneous here.
                Iceberg::FileContentType replacement_content_type = plan.survivors.front()->parsed_entry->content_type;
                for (const auto & s : plan.survivors)
                {
                    if (s->parsed_entry->content_type != replacement_content_type)
                        throw Exception(
                            ErrorCodes::NOT_IMPLEMENTED,
                            "Manifest {} mixes content types; rewriting it is not supported",
                            plan.path.serialize());
                }

                auto new_manifest_path = filename_generator.generateManifestEntryName();
                const String new_storage_path = persistent_components.path_resolver.resolve(new_manifest_path);
                wrote_for_cleanup.push_back(new_storage_path);

                auto buf = object_storage->writeObject(
                    StoredObject(new_storage_path),
                    WriteMode::Rewrite,
                    std::nullopt,
                    DBMS_DEFAULT_BUFFER_SIZE,
                    context->getWriteSettings());

                generateExistingManifestFile(
                    metadata_object,
                    partition_spec,
                    partition_spec_id,
                    partition_columns,
                    partition_types,
                    plan.survivors,
                    *buf);
                buf->finalize();

                Int64 length = buf->count();
                if (length == 0)
                    length = object_storage->getObjectMetadata(new_storage_path, /*with_tags=*/false).size_bytes;

                /// Mark the original manifest as removed from the new list.
                skip_manifest_paths.insert(persistent_components.path_resolver.resolve(plan.path));

                RewriteOutput out;
                out.new_manifest_path = new_manifest_path;
                out.manifest_length = length;
                out.existing_files_count = static_cast<Int32>(plan.survivors.size());
                Int64 row_total = 0;
                for (const auto & s : plan.survivors)
                    row_total += s->parsed_entry->record_count;
                out.existing_rows_count = static_cast<Int32>(row_total);
                /// Smallest entry sequence number — describes the contents, not the
                /// new manifest file (which is added by the current DROP snapshot).
                Int64 min_entry_seq = std::numeric_limits<Int64>::max();
                for (const auto & s : plan.survivors)
                {
                    Int64 seq = s->parsed_entry->parsed_sequence_number.value_or(s->sequence_number);
                    min_entry_seq = std::min(min_entry_seq, seq);
                }
                if (min_entry_seq == std::numeric_limits<Int64>::max())
                    min_entry_seq = 0;
                out.min_sequence_number = min_entry_seq;
                out.content_type = replacement_content_type;
                rewrites.push_back(out);
            }

            /// Build the new snapshot.
            MetadataGenerator metadata_generator(metadata_object);
            auto metadata_info = filename_generator.generateMetadataPathWithInfo();
            Int64 parent_snapshot_id = -1;
            if (metadata_object->has(Iceberg::f_current_snapshot_id))
                parent_snapshot_id = metadata_object->getValue<Int64>(Iceberg::f_current_snapshot_id);

            auto new_snapshot_result = metadata_generator.generateNextMetadataForDelete(
                filename_generator,
                metadata_info.path,
                parent_snapshot_id,
                removed_data_files,
                removed_records,
                removed_files_size,
                removed_position_delete_files,
                removed_position_deletes,
                static_cast<Int64>(changed_partitions.size()));

            const String storage_manifest_list_path = persistent_components.path_resolver.resolve(new_snapshot_result.manifest_list_path);
            wrote_for_cleanup.push_back(storage_manifest_list_path);

            /// Assemble manifest list entries for the rewritten survivor manifests.
            std::vector<ManifestListEntryForDelete> new_entries;
            new_entries.reserve(rewrites.size());
            for (const auto & r : rewrites)
            {
                ManifestListEntryForDelete e;
                e.manifest_path = r.new_manifest_path;
                e.manifest_length = r.manifest_length;
                e.min_sequence_number = r.min_sequence_number;
                e.added_files_count = 0;
                e.existing_files_count = r.existing_files_count;
                e.deleted_files_count = 0;
                e.added_rows_count = 0;
                e.existing_rows_count = r.existing_rows_count;
                e.deleted_rows_count = 0;
                e.content_type = r.content_type;
                new_entries.push_back(e);
            }

            {
                auto buf = object_storage->writeObject(
                    StoredObject(storage_manifest_list_path),
                    WriteMode::Rewrite,
                    std::nullopt,
                    DBMS_DEFAULT_BUFFER_SIZE,
                    context->getWriteSettings());

                generateManifestListForDelete(
                    persistent_components.path_resolver,
                    metadata_object,
                    object_storage,
                    context,
                    new_snapshot_result.snapshot,
                    new_entries,
                    skip_manifest_paths,
                    *buf);
                buf->finalize();
            }

            /// Commit the new metadata.json via the version-hint / CAS path.
            std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            Poco::JSON::Stringifier::stringify(metadata_object, oss, 4);
            std::string json_representation = removeEscapedSlashes(oss.str());

            fiu_do_on(FailPoints::iceberg_writes_cleanup,
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failpoint for cleanup enabled");
            });

            auto hint_path = filename_generator.generateVersionHint();
            const bool committed = writeMetadataFileAndVersionHint(
                persistent_components.path_resolver,
                metadata_info,
                json_representation,
                hint_path,
                object_storage,
                context,
                data_lake_settings[DataLakeStorageSetting::iceberg_use_version_hint]);

            if (!committed)
            {
                /// Lost CAS race; another writer committed between our state-fetch and
                /// metadata write. Clean up our partial files and retry — the target
                /// data_paths_to_remove set is preserved across attempts.
                cleanup();
                continue;
            }

            LOG_INFO(log, "DROP PARTITION committed: removed {} data files ({} rows), {} position-delete files",
                     removed_data_files, removed_records, removed_position_delete_files);
            return;
        }
        catch (...)
        {
            cleanup();
            throw;
        }
    }

    throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Too many retries to commit Iceberg DROP PARTITION");
#else
    (void)command; (void)context;
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Iceberg DROP PARTITION requires USE_AVRO");
#endif
}

}
