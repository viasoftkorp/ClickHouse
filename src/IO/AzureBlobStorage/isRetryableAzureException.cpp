#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <IO/AzureBlobStorage/isRetryableAzureException.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int PATH_ACCESS_DENIED;
}

bool isRetryableAzureException(const Azure::Core::RequestFailedException & e)
{
    /// Always retry transport errors.
    if (dynamic_cast<const Azure::Core::Http::TransportException *>(&e))
        return true;

    /// Azure may be provisioning access for quite a long time, so 403 is always treated as retryable.
    /// Without this, a brief permission-propagation window after a credential/role change causes
    /// in-flight SELECTs to fail and, worse, to be reclassified as POTENTIALLY_BROKEN_DATA_PART by
    /// MergeTreeSequentialSource -> StorageSharedMergeTree::reportBrokenPart, which triggers loud
    /// alerting via ForcedCriticalErrorsLogger even though the underlying part is fine.
    /// A genuinely permanent 403 still surfaces: in-buffer retries are bounded, and the final
    /// failure is reported with code PATH_ACCESS_DENIED (see `rethrowAzureException`) rather than
    /// a phantom broken-part error.
    if (e.StatusCode == Azure::Core::Http::HttpStatusCode::Forbidden)
        return true;

    /// Retry other 5xx errors just in case.
    return e.StatusCode >= Azure::Core::Http::HttpStatusCode::InternalServerError;
}

bool isAzureForbiddenException(const Azure::Core::RequestFailedException & e)
{
    return e.StatusCode == Azure::Core::Http::HttpStatusCode::Forbidden;
}

[[noreturn]] void rethrowAzureException(
    const Azure::Core::RequestFailedException & e,
    const std::string & resource)
{
    if (isAzureForbiddenException(e))
        throw Exception(
            ErrorCodes::PATH_ACCESS_DENIED,
            "Azure refused access to `{}`: {} (HTTP {}, request id {})",
            resource,
            e.Message,
            static_cast<int>(e.StatusCode),
            e.RequestId);

    throw;
}

}

#endif
