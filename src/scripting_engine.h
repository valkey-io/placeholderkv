#ifndef _ENGINE_H_
#define _ENGINE_H_

#include "server.h"

// Forward declaration of the engine structure.
typedef struct scriptingEngine scriptingEngine;

/* ValkeyModule type aliases for scripting engine structs and types. */
typedef ValkeyModuleScriptingEngineCtx engineCtx;
typedef ValkeyModuleScriptingEngineFunctionCtx functionCtx;
typedef ValkeyModuleScriptingEngineCompiledFunction compiledFunction;
typedef ValkeyModuleScriptingEngineMemoryInfo engineMemoryInfo;
typedef ValkeyModuleScriptingEngineMethods engineMethods;

/*
 * Callback function used to iterate the list of engines registered in the
 * engine manager.
 *
 * - `engine`: the scripting engine in the current iteration.
 *
 * - `context`: a generic pointer to a context object.
 *
 * If the callback function returns 0, then the iteration is stopped
 * immediately.
 */
typedef int (*engineIterCallback)(scriptingEngine *engine, void *context);

/*
 * Engine manager API functions.
 */
int engineManagerInit(void);
size_t engineManagerGetCacheMemory(void);
size_t engineManagerGetNumEngines(void);
size_t engineManagerGetMemoryUsage(void);
int engineManagerRegisterEngine(const char *engine_name,
                                ValkeyModule *engine_module,
                                engineCtx *engine_ctx,
                                engineMethods *engine_methods);
int engineManagerUnregisterEngine(const char *engine_name);
scriptingEngine *engineManagerFind(sds engine_name);
void engineManagerForEachEngine(engineIterCallback callback, void *context);

/*
 * Engine API functions.
 */
sds engineGetName(scriptingEngine *engine);
client *engineGetClient(scriptingEngine *engine);
ValkeyModule *engineGetModule(scriptingEngine *engine);

/*
 * API to call engine callback functions.
 */
compiledFunction **engineCallCreateFunctionsLibrary(scriptingEngine *engine,
                                                    const char *code,
                                                    size_t timeout,
                                                    size_t *out_num_compiled_functions,
                                                    robj **err);
void engineCallFunction(scriptingEngine *engine,
                        functionCtx *func_ctx,
                        client *caller,
                        void *compiled_function,
                        robj **keys,
                        size_t nkeys,
                        robj **args,
                        size_t nargs);
void engineCallFreeFunction(scriptingEngine *engine, void *compiled_func);
size_t engineCallGetFunctionMemoryOverhead(scriptingEngine *engine,
                                           void *compiled_function);
engineMemoryInfo engineCallGetMemoryInfo(scriptingEngine *engine);

#endif /* _ENGINE_H_ */
