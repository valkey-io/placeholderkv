#include "engine.h"
#include "dict.h"
#include "functions.h"
#include "module.h"

typedef struct engineImpl {
    /* Engine specific context */
    engineCtx *ctx;

    /* Callback functions implemented by the scripting engine module */
    engineMethods methods;
} engineImpl;

typedef struct engine {
    sds name;                    /* Name of the engine */
    ValkeyModule *module;        /* the module that implements the scripting engine */
    engineImpl *impl;            /* engine callbacks that allows to interact with the engine */
    client *c;                   /* Client that is used to run commands */
    ValkeyModuleCtx *module_ctx; /* Cache of the module context object */
} engine;


typedef struct engineManger {
    dict *engines; /* engines dictionary */
    size_t engine_cache_memory;
} engineManager;


static engineManager engineMgr = {
    .engines = NULL,
    .engine_cache_memory = 0,
};

static uint64_t dictStrCaseHash(const void *key) {
    return dictGenCaseHashFunction((unsigned char *)key, strlen((char *)key));
}

dictType engineDictType = {
    dictStrCaseHash,       /* hash function */
    NULL,                  /* key dup */
    dictSdsKeyCaseCompare, /* key compare */
    NULL,                  /* key destructor */
    NULL,                  /* val destructor */
    NULL                   /* allow to expand */
};

/* Initializes the scripting engine manager.
 * The engine manager is responsible for managing the several scripting engines
 * that are loaded in the server and implemented by Valkey Modules.
 *
 * Returns C_ERR if some error occurs during the initialization.
 */
int engineManagerInit(void) {
    engineMgr.engines = dictCreate(&engineDictType);
    return C_OK;
}

size_t engineManagerGetCacheMemory(void) {
    return engineMgr.engine_cache_memory;
}

size_t engineManagerGetNumEngines(void) {
    return dictSize(engineMgr.engines);
}

size_t engineManagerGetMemoryUsage(void) {
    return dictMemUsage(engineMgr.engines) + sizeof(engineMgr);
}

/* Registers a new scripting engine in the engine manager.
 *
 * - `engine_name`: the name of the scripting engine. This name will match
 * against the engine name specified in the script header using a shebang.
 *
 * - `ctx`: engine specific context pointer.
 *
 * - engine_methods - the struct with the scripting engine callback functions
 * pointers.
 *
 * Returns C_ERR in case of an error during registration.
 */
int engineManagerRegisterEngine(const char *engine_name,
                                ValkeyModule *engine_module,
                                engineCtx *engine_ctx,
                                engineMethods *engine_methods) {
    sds engine_name_sds = sdsnew(engine_name);

    if (dictFetchValue(engineMgr.engines, engine_name_sds)) {
        serverLog(LL_WARNING, "Same engine was registered twice");
        sdsfree(engine_name_sds);
        return C_ERR;
    }

    engineImpl *ei = zmalloc(sizeof(engineImpl));
    *ei = (engineImpl){
        .ctx = engine_ctx,
        .methods = {
            .create_functions_library = engine_methods->create_functions_library,
            .call_function = engine_methods->call_function,
            .free_function = engine_methods->free_function,
            .get_function_memory_overhead = engine_methods->get_function_memory_overhead,
            .get_memory_info = engine_methods->get_memory_info,
        },
    };

    client *c = createClient(NULL);
    c->flag.deny_blocking = 1;
    c->flag.script = 1;
    c->flag.fake = 1;

    engine *e = zmalloc(sizeof(*ei));
    *e = (engine){
        .name = engine_name_sds,
        .module = engine_module,
        .impl = ei,
        .c = c,
        .module_ctx = engine_module ? moduleAllocateContext() : NULL,
    };

    dictAdd(engineMgr.engines, engine_name_sds, e);

    engineMemoryInfo mem_info = engineCallGetMemoryInfo(e);
    engineMgr.engine_cache_memory += zmalloc_size(e) +
                                     sdsAllocSize(e->name) +
                                     zmalloc_size(ei) +
                                     mem_info.engine_memory_overhead;

    return C_OK;
}

/* Removes a scripting engine from the engine manager.
 *
 * - `engine_name`: name of the engine to remove
 */
int engineManagerUnregisterEngine(const char *engine_name) {
    dictEntry *entry = dictUnlink(engineMgr.engines, engine_name);
    if (entry == NULL) {
        serverLog(LL_WARNING, "There's no engine registered with name %s", engine_name);
        return C_ERR;
    }

    engine *e = dictGetVal(entry);

    functionsRemoveLibFromEngine(e);

    zfree(e->impl);
    sdsfree(e->name);
    freeClient(e->c);
    if (e->module_ctx) {
        serverAssert(e->module != NULL);
        zfree(e->module_ctx);
    }
    zfree(e);

    dictFreeUnlinkedEntry(engineMgr.engines, entry);

    return C_OK;
}

/*
 * Lookups the engine with `engine_name` in the engine manager and returns it if
 * it exists. Otherwise returns `NULL`.
 */
engine *engineManagerFind(sds engine_name) {
    dictEntry *entry = dictFind(engineMgr.engines, engine_name);
    if (entry) {
        return dictGetVal(entry);
    }
    return NULL;
}

sds engineGetName(engine *engine) {
    return engine->name;
}

client *engineGetClient(engine *engine) {
    return engine->c;
}

ValkeyModule *engineGetModule(engine *engine) {
    return engine->module;
}

/*
 * Iterates the list of engines registered in the engine manager and calls the
 * callback function with each engine.
 *
 * The `context` pointer is also passed in each callback call.
 */
void engineManagerForEachEngine(engineIterCallback callback, void *context) {
    dictIterator *iter = dictGetIterator(engineMgr.engines);
    dictEntry *entry = NULL;
    while ((entry = dictNext(iter))) {
        engine *e = dictGetVal(entry);
        if (!callback(e, context)) {
            break;
        }
    }
    dictReleaseIterator(iter);
}

static void engineSetupModuleCtx(engine *e, client *c) {
    if (e->module != NULL) {
        serverAssert(e->module_ctx != NULL);
        moduleScriptingEngineInitContext(e->module_ctx, e->module, c);
    }
}

static void engineTeardownModuleCtx(engine *e) {
    if (e->module != NULL) {
        serverAssert(e->module_ctx != NULL);
        moduleFreeContext(e->module_ctx);
    }
}

compiledFunction **engineCallCreateFunctionsLibrary(engine *engine,
                                                    const char *code,
                                                    size_t timeout,
                                                    size_t *out_num_compiled_functions,
                                                    robj **err) {
    engineSetupModuleCtx(engine, NULL);

    compiledFunction **functions = engine->impl->methods.create_functions_library(
        engine->module_ctx,
        engine->impl->ctx,
        code,
        timeout,
        out_num_compiled_functions,
        err);

    engineTeardownModuleCtx(engine);

    return functions;
}

void engineCallFunction(engine *engine,
                        functionCtx *func_ctx,
                        client *caller,
                        void *compiled_function,
                        robj **keys,
                        size_t nkeys,
                        robj **args,
                        size_t nargs) {
    engineSetupModuleCtx(engine, caller);

    engine->impl->methods.call_function(
        engine->module_ctx,
        engine->impl->ctx,
        func_ctx,
        compiled_function,
        keys,
        nkeys,
        args,
        nargs);

    engineTeardownModuleCtx(engine);
}

void engineCallFreeFunction(engine *engine,
                            void *compiled_func) {
    engineSetupModuleCtx(engine, NULL);
    engine->impl->methods.free_function(engine->module_ctx,
                                        engine->impl->ctx,
                                        compiled_func);
    engineTeardownModuleCtx(engine);
}

size_t engineCallGetFunctionMemoryOverhead(engine *engine,
                                           void *compiled_function) {
    engineSetupModuleCtx(engine, NULL);
    size_t mem = engine->impl->methods.get_function_memory_overhead(
        engine->module_ctx, compiled_function);
    engineTeardownModuleCtx(engine);
    return mem;
}

engineMemoryInfo engineCallGetMemoryInfo(engine *engine) {
    engineSetupModuleCtx(engine, NULL);
    engineMemoryInfo mem_info = engine->impl->methods.get_memory_info(
        engine->module_ctx, engine->impl->ctx);
    engineTeardownModuleCtx(engine);
    return mem_info;
}
