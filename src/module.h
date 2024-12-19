#ifndef _MODULE_H_
#define _MODULE_H_

/* This header file exposes a set of functions defined in module.c that are
 * not part of the module API, but are used by the core to interact with modules
 */

typedef struct ValkeyModuleCtx ValkeyModuleCtx;
typedef struct ValkeyModule ValkeyModule;

ValkeyModuleCtx *moduleAllocateScriptingEngineContext(ValkeyModule *module);
void moduleFreeContext(ValkeyModuleCtx *ctx);

#endif /* _MODULE_H_ */
