/*
 Includes only polaris_abi.h, so a header that leans on an include of its
 includer fails to compile. The function below uses the plugin ABI the way
 a host does: it checks the version before any entry, then calls through
 the vtable.
 */
#include "polaris_abi.h"

int32_t polaris_create_through(polaris_adapter_entry_fn entry, const char *config_json,
                               polaris_adapter **out);

int32_t polaris_create_through(polaris_adapter_entry_fn entry, const char *config_json,
                               polaris_adapter **out) {
    const polaris_adapter_vtable *vtable = entry();
    const polaris_record *records = NULL;
    size_t written = 0;
    int32_t rc;
    if (vtable->abi_version != POLARIS_PLUGIN_ABI_VERSION) {
        return POLARIS_PLUGIN_ERR_NULL;
    }
    rc = vtable->create(config_json, out);
    if (rc != POLARIS_PLUGIN_OK) {
        return rc;
    }
    rc = vtable->drain(*out, &records, &written);
    if (rc == POLARIS_PLUGIN_OK && written > 0) {
        const polaris_record_header *header = (const polaris_record_header *)records[0].bytes;
        uint32_t words = header->length;
        if (words * 4u != records[0].len) {
            return POLARIS_PLUGIN_ERR_NULL;
        }
    }
    return rc;
}
