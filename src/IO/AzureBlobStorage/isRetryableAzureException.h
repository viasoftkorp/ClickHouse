#pragma once
#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <azure/core/http/http.hpp>
#include <string>

namespace DB
{

bool isRetryableAzureException(const Azure::Core::RequestFailedException & e);

bool isAzureForbiddenException(const Azure::Core::RequestFailedException & e);

/// Rethrow a caught Azure exception, converting 403 Forbidden to PATH_ACCESS_DENIED so
/// downstream classification (broken-part detection, alerts) doesn't conflate it with data
/// corruption. Must be called from inside an active catch handler.
[[noreturn]] void rethrowAzureException(
    const Azure::Core::RequestFailedException & e,
    const std::string & resource);

}

#endif
