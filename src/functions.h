/*
 * Copyright (c) 2021, Redis Ltd.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __FUNCTIONS_H_
#define __FUNCTIONS_H_

/*
 * functions.c unit provides the Functions API:
 * * FUNCTION LOAD
 * * FUNCTION LIST
 * * FUNCTION CALL (FCALL and FCALL_RO)
 * * FUNCTION DELETE
 * * FUNCTION STATS
 * * FUNCTION KILL
 * * FUNCTION FLUSH
 * * FUNCTION DUMP
 * * FUNCTION RESTORE
 * * FUNCTION HELP
 *
 * Also contains implementation for:
 * * Save/Load function from rdb
 * * Register engines
 */

#include "server.h"
#include "script.h"
#include "valkeymodule.h"

typedef struct functionLibInfo functionLibInfo;

/* ValkeyModule type aliases for scripting engine structs and types. */
typedef ValkeyModuleScriptingEngineCtx engineCtx;
typedef ValkeyModuleScriptingEngineFunctionCtx functionCtx;
typedef ValkeyModuleScriptingEngineCompiledFunction compiledFunction;
typedef ValkeyModuleScriptingEngineMemoryInfo engineMemoryInfo;
typedef ValkeyModuleScriptingEngineMethods engineMethods;

typedef struct engine {
    /* engine specific context */
    engineCtx *engine_ctx;

    /* Compiles the script code and returns an array of compiled functions
     * registered in the script./
     *
     * Returns NULL on error and set err to be the error message */
    compiledFunction **(*create)(
        ValkeyModuleCtx *module_ctx,
        engineCtx *engine_ctx,
        const char *code,
        size_t timeout,
        size_t *out_num_compiled_functions,
        char **err);

    /* Invoking a function, func_ctx is an opaque object (from engine POV).
     * The func_ctx should be used by the engine to interaction with the server,
     * such interaction could be running commands, set resp, or set
     * replication mode
     */
    void (*call)(ValkeyModuleCtx *module_ctx,
                 engineCtx *engine_ctx,
                 functionCtx *func_ctx,
                 void *compiled_function,
                 robj **keys,
                 size_t nkeys,
                 robj **args,
                 size_t nargs);

    /* free the given function */
    void (*free_function)(ValkeyModuleCtx *module_ctx,
                          engineCtx *engine_ctx,
                          void *compiled_function);

    /* Return memory overhead for a given function,
     * such memory is not counted as engine memory but as general
     * structs memory that hold different information */
    size_t (*get_function_memory_overhead)(ValkeyModuleCtx *module_ctx,
                                           void *compiled_function);

    /* Get the current used memory by the engine */
    engineMemoryInfo (*get_memory_info)(ValkeyModuleCtx *module_ctx,
                                        engineCtx *engine_ctx);

} engine;

/* Hold information about an engine.
 * Used on rdb.c so it must be declared here. */
typedef struct engineInfo {
    sds name;                    /* Name of the engine */
    ValkeyModule *engineModule;  /* the module that implements the scripting engine */
    ValkeyModuleCtx *module_ctx; /* Scripting engine module context */
    engine *engine;              /* engine callbacks that allows to interact with the engine */
    client *c;                   /* Client that is used to run commands */
} engineInfo;

/* Hold information about the specific function.
 * Used on rdb.c so it must be declared here. */
typedef struct functionInfo {
    sds name;            /* Function name */
    void *function;      /* Opaque object that set by the function's engine and allow it
                            to run the function, usually it's the function compiled code. */
    functionLibInfo *li; /* Pointer to the library created the function */
    sds desc;            /* Function description */
    uint64_t f_flags;    /* Function flags */
} functionInfo;

/* Hold information about the specific library.
 * Used on rdb.c so it must be declared here. */
struct functionLibInfo {
    sds name;        /* Library name */
    dict *functions; /* Functions dictionary */
    engineInfo *ei;  /* Pointer to the function engine */
    sds code;        /* Library code */
};

int functionsRegisterEngine(const char *engine_name,
                            ValkeyModule *engine_module,
                            void *engine_ctx,
                            engineMethods *engine_methods);
int functionsUnregisterEngine(const char *engine_name);

sds functionsCreateWithLibraryCtx(sds code, int replace, sds *err, functionsLibCtx *lib_ctx, size_t timeout);
unsigned long functionsMemory(void);
unsigned long functionsMemoryOverhead(void);
unsigned long functionsNum(void);
unsigned long functionsLibNum(void);
dict *functionsLibGet(void);
size_t functionsLibCtxFunctionsLen(functionsLibCtx *functions_ctx);
functionsLibCtx *functionsLibCtxGetCurrent(void);
functionsLibCtx *functionsLibCtxCreate(void);
void functionsLibCtxClearCurrent(int async, void(callback)(dict *));
void functionsLibCtxFree(functionsLibCtx *functions_lib_ctx);
void functionsLibCtxClear(functionsLibCtx *lib_ctx, void(callback)(dict *));
void functionsLibCtxSwapWithCurrent(functionsLibCtx *new_lib_ctx, int async);

int luaEngineInitEngine(void);
int functionsInit(void);

#endif /* __FUNCTIONS_H_ */
