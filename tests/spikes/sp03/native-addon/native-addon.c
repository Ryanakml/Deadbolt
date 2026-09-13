#include <node_api.h>

static napi_value target_arch(napi_env env, napi_callback_info info) {
  napi_value value;
  napi_create_string_utf8(env, "native-addon-loaded", NAPI_AUTO_LENGTH, &value);
  return value;
}

static napi_value init(napi_env env, napi_value exports) {
  napi_value fn;
  napi_create_function(env, "targetArch", NAPI_AUTO_LENGTH, target_arch, NULL, &fn);
  napi_set_named_property(env, exports, "targetArch", fn);
  return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, init)
