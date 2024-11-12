#include "valkeymodule.h"

#include <string.h>

int testadminport_runspecificcommand(ValkeyModuleCtx *ctx,
                                     ValkeyModuleString **argv, int argc) {
  VALKEYMODULE_NOT_USED(argv);
  VALKEYMODULE_NOT_USED(argc);
  int port = ValkeyModule_GetClientConnectedPort(ctx);
  if (port == 7001) {
    ValkeyModule_ReplyWithSimpleString(ctx, "You can execute this command");
  } else {
    ValkeyModule_ReplyWithSimpleString(
        ctx, "You have no permission to execute this command");
  }
  return VALKEYMODULE_OK;
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                        int argc) {
  VALKEYMODULE_NOT_USED(argv);
  VALKEYMODULE_NOT_USED(argc);
  if (ValkeyModule_Init(ctx, "adminport", 1, VALKEYMODULE_APIVER_1) ==
      VALKEYMODULE_ERR)
    return VALKEYMODULE_ERR;

  if (ValkeyModule_CreateCommand(ctx, "testadminport.runspecificcommand",
                                 testadminport_runspecificcommand, "", 0, 0,
                                 0) == VALKEYMODULE_ERR)
    return VALKEYMODULE_ERR;

  return VALKEYMODULE_OK;
}
