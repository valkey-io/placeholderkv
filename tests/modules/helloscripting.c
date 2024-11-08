#include "valkeymodule.h"

#include <string.h>
#include <ctype.h>
#include <errno.h>


typedef enum HelloInstKind {
    FUNCTION = 0,
    CONSTI,
    ARGS,
    RETURN,
    _END,
} HelloInstKind;

const char *HelloInstKindStr[] = {
    "FUNCTION",
    "CONSTI",
    "ARGS",
    "RETURN",
};

typedef struct HelloInst {
    HelloInstKind kind;
    union {
        uint32_t integer;
        const char *string;
    } param;
} HelloInst;

typedef struct HelloFunc {
    char *name;
    HelloInst instructions[256];
    uint32_t num_instructions;
} HelloFunc;

typedef struct HelloProgram {
    HelloFunc *functions[16];
    uint32_t num_functions;
} HelloProgram;

typedef struct HelloLangCtx {
    HelloProgram *program;
} HelloLangCtx;


static HelloLangCtx *hello_ctx = NULL;


static HelloInstKind helloLangParseInstruction(const char *token) {
    for (HelloInstKind i = 0; i < _END; i++) {
        if (strcmp(HelloInstKindStr[i], token) == 0) {
            return i;
        }
    }
    return _END;
}

static void helloLangParseFunction(HelloFunc *func) {
    char *token = strtok(NULL, " \n");
    ValkeyModule_Assert(token != NULL);
    func->name = ValkeyModule_Alloc(sizeof(char) * strlen(token) + 1);
    strcpy(func->name, token);
}

static uint32_t str2int(const char *str) {
    char *end;
    errno = 0;
    uint32_t val = (uint32_t)strtoul(str, &end, 10);
    ValkeyModule_Assert(errno == 0);
    return val;
}

static void helloLangParseIntegerParam(HelloFunc *func) {
    char *token = strtok(NULL, " \n");
    func->instructions[func->num_instructions].param.integer = str2int(token);
}

static void helloLangParseConstI(HelloFunc *func) {
    helloLangParseIntegerParam(func);
    func->num_instructions++;
}

static void helloLangParseArgs(HelloFunc *func) {
    helloLangParseIntegerParam(func);
    func->num_instructions++;
}

static HelloProgram *helloLangParseCode(const char *code, HelloProgram *program) {
    char *_code = ValkeyModule_Alloc(sizeof(char) * strlen(code) + 1);
    strcpy(_code, code);

    HelloFunc *currentFunc = NULL;

    char *token = strtok(_code, " \n");
    while (token != NULL) {
        HelloInstKind kind = helloLangParseInstruction(token);

        if (currentFunc != NULL) {
            currentFunc->instructions[currentFunc->num_instructions].kind = kind;
        }

        switch (kind) {
        case FUNCTION:
            ValkeyModule_Assert(currentFunc == NULL);
            currentFunc = ValkeyModule_Alloc(sizeof(HelloFunc));
            program->functions[program->num_functions++] = currentFunc;
            helloLangParseFunction(currentFunc);
            break;
        case CONSTI:
            ValkeyModule_Assert(currentFunc != NULL);
            helloLangParseConstI(currentFunc);
            break;
        case ARGS:
            ValkeyModule_Assert(currentFunc != NULL);
            helloLangParseArgs(currentFunc);
            break;
        case RETURN:
            ValkeyModule_Assert(currentFunc != NULL);
            currentFunc->num_instructions++;
            currentFunc = NULL;
            break;
        case _END:
            ValkeyModule_Assert(0);
        }

        token = strtok(NULL, " \n");
    }

    ValkeyModule_Free(_code);

    return program;
}

static uint32_t executeHelloLangFunction(HelloFunc *func, ValkeyModuleString **args, int nargs) {
    uint32_t stack[64];
    int sp = 0;

    for (uint32_t pc = 0; pc < func->num_instructions; pc++) {
        HelloInst instr = func->instructions[pc];
        switch (instr.kind) {
        case CONSTI:
            stack[sp++] = instr.param.integer;
            break;
        case ARGS:
            uint32_t idx = instr.param.integer;
            ValkeyModule_Assert(idx < (uint32_t)nargs);
            size_t len;
            const char *argStr = ValkeyModule_StringPtrLen(args[idx], &len);
            uint32_t arg = str2int(argStr);
            stack[sp++] = arg;
            break;
        case RETURN:
            uint32_t val = stack[--sp];
            ValkeyModule_Assert(sp == 0);
            return val;
        case FUNCTION:
        case _END:
            ValkeyModule_Assert(0);
        }
    }

    ValkeyModule_Assert(0);
    return 0;
}

static size_t engineGetUsedMemoy(void *engine_ctx) {
    VALKEYMODULE_NOT_USED(engine_ctx);
    return 0;
}

static size_t engineMemoryOverhead(void *engine_ctx) {
    HelloLangCtx *ctx = (HelloLangCtx *)engine_ctx;
    size_t overhead = ValkeyModule_MallocSize(engine_ctx);
    if (ctx->program != NULL) {
        overhead += ValkeyModule_MallocSize(ctx->program);
    }
    return overhead;
}

static size_t engineFunctionMemoryOverhead(void *compiled_function) {
    HelloFunc *func = (HelloFunc *)compiled_function;
    return ValkeyModule_MallocSize(func->name);
}

static void engineFreeFunction(void *engine_ctx, void *compiled_function) {
    VALKEYMODULE_NOT_USED(engine_ctx);
    HelloFunc *func = (HelloFunc *)compiled_function;
    ValkeyModule_Free(func->name);
    func->name = NULL;
    ValkeyModule_Free(func);
}

static int createHelloLangEngine(void *engine_ctx, ValkeyModuleScriptingEngineFunctionLibrary *li, const char *code, size_t timeout, char **err) {
    VALKEYMODULE_NOT_USED(timeout);

    HelloLangCtx *ctx = (HelloLangCtx *)engine_ctx;

    if (ctx->program == NULL) {
        ctx->program = ValkeyModule_Alloc(sizeof(HelloProgram));
        memset(ctx->program, 0, sizeof(HelloProgram));
    } else {
        ctx->program->num_functions = 0;
    }

    ctx->program = helloLangParseCode(code, ctx->program);

    for (uint32_t i = 0; i < ctx->program->num_functions; i++) {
        HelloFunc *func = ctx->program->functions[i];
        int ret = ValkeyModule_RegisterScriptingEngineFunction(func->name, func, li, NULL, 0, err);
        if (ret != 0) {
            // We need to cleanup all parsed functions that were not registered.
            for (uint32_t j=i; j < ctx->program->num_functions; j++) {
                engineFreeFunction(NULL, ctx->program->functions[j]);
            }
            return ret;
        }
    }

    return 0;
}

static void callHelloLangFunction(ValkeyModuleScriptingEngineFunctionCallCtx *func_ctx,
                                  void *engine_ctx,
                                  void *compiled_function,
                                  ValkeyModuleString **keys,
                                  size_t nkeys,
                                  ValkeyModuleString **args,
                                  size_t nargs) {
    VALKEYMODULE_NOT_USED(engine_ctx);
    VALKEYMODULE_NOT_USED(keys);
    VALKEYMODULE_NOT_USED(nkeys);

    ValkeyModuleCtx *ctx = ValkeyModule_GetModuleCtxFromFunctionCallCtx(func_ctx);

    HelloFunc *func = (HelloFunc *)compiled_function;
    uint32_t result = executeHelloLangFunction(func, args, nargs);

    ValkeyModule_ReplyWithLongLong(ctx, result);
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    if (ValkeyModule_Init(ctx, "helloengine", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR) return VALKEYMODULE_ERR;

    hello_ctx = ValkeyModule_Alloc(sizeof(HelloLangCtx));
    hello_ctx->program = NULL;

    ValkeyModule_RegisterScriptingEngine(ctx,
                                         "HELLO",
                                         hello_ctx,
                                         createHelloLangEngine,
                                         callHelloLangFunction,
                                         engineGetUsedMemoy,
                                         engineFunctionMemoryOverhead,
                                         engineMemoryOverhead,
                                         engineFreeFunction);

    return VALKEYMODULE_OK;
}

int ValkeyModule_OnUnload(ValkeyModuleCtx *ctx) {
    if (ValkeyModule_UnregisterScriptingEngine(ctx, "HELLO") != VALKEYMODULE_OK) {
        ValkeyModule_Log(ctx, "error", "Failed to unregister engine");
        return VALKEYMODULE_ERR;
    }

    ValkeyModule_Free(hello_ctx->program);
    hello_ctx->program = NULL;
    ValkeyModule_Free(hello_ctx);
    hello_ctx = NULL;

    return VALKEYMODULE_OK;
}
