#include "valkeymodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

int GET_HELLO(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
  ValkeyModule_UpdateRunTimeArgs(ctx, argc, argv);
  return ValkeyModule_ReplyWithSimpleString(ctx, "Module runtime args test");
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    if (ValkeyModule_Init(ctx,"myhello",1,VALKEYMODULE_APIVER_1)
        == VALKEYMODULE_ERR) return VALKEYMODULE_ERR;


    if (ValkeyModule_CreateCommand(ctx,"hello.hi",
        GET_HELLO,"fast",0,0,0) == VALKEYMODULE_ERR)
       return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}
