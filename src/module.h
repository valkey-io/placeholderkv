#ifndef _MODULE_H_
#define _MODULE_H_

typedef struct ValkeyModuleCtx ValkeyModuleCtx;
typedef struct ValkeyModule ValkeyModule;
typedef struct client client;

ValkeyModuleCtx *moduleAllocateContext(void);
void moduleScriptingEngineInitContext(ValkeyModuleCtx *out_ctx,
                                      ValkeyModule *module,
                                      client *client);
void moduleFreeContext(ValkeyModuleCtx *ctx);

#endif /* _MODULE_H_ */
