#ifndef _ENGINE_H_
#define _ENGINE_H_

#include "server.h"

// Forward declaration of the engine structure.
typedef struct engine engine;

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
 * - `engine`: the engine in the current iteration.
 *
 * - `context`: a generic pointer to a context object.
 *
 * If the callback function returns 0, then the iteration is stopped
 * immediately.
 */
typedef int (*engineIterCallback)(engine *engine, void *context);

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
engine *engineManagerFind(sds engine_name);
void engineManagerForEachEngine(engineIterCallback callback, void *context);

/*
 * Engine API functions.
 */
sds engineGetName(engine *engine);
client *engineGetClient(engine *engine);
ValkeyModule *engineGetModule(engine *engine);

/*
 * API to call engine callback functions.
 */
compiledFunction **engineCallCreateFunctionsLibrary(engine *engine,
                                                    const char *code,
                                                    size_t timeout,
                                                    size_t *out_num_compiled_functions,
                                                    robj **err);
void engineCallFunction(engine *engine,
                        functionCtx *func_ctx,
                        client *caller,
                        void *compiled_function,
                        robj **keys,
                        size_t nkeys,
                        robj **args,
                        size_t nargs);
void engineCallFreeFunction(engine *engine, void *compiled_func);
size_t engineCallGetFunctionMemoryOverhead(engine *engine, void *compiled_function);
engineMemoryInfo engineCallGetMemoryInfo(engine *engine);

#endif /* _ENGINE_H_ */
