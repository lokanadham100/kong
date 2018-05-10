local responses = require "kong.tools.responses"
local singletons = require "kong.singletons"


local ngx_thread_spawn = ngx.thread.spawn
local ngx_thread_wait = ngx.thread.wait
local table_insert = table.insert


local function parallelize(operations)
  local results = {}
  for i = 1, #operations do
    local op = operations[i]
    local co = ngx_thread_spawn(op[1], op[2], op[3], op[4])
    if coroutine.status(co) == "zombie" then
      local _, r = ngx_thread_wait(co)
      results[i] = r
    else
      results[i] = co
    end
  end
  return results
end


local function get_result(results, i)
  local r = results[i]
  if type(r) == "thread" then
    local _
    _, r = ngx_thread_wait(r)
    results[i] = r
  end
  return r
end


-- Loads a plugin config from the datastore.
-- @return plugin config table or an empty sentinel table in case of a db-miss
local function load_plugin_into_memory(api_id, consumer_id, plugin_name)
  local rows, err = singletons.dao.plugins:find_all {
    api_id = api_id,
    consumer_id = consumer_id,
    name = plugin_name
  }
  if err then
    error(err)
  end

  if #rows > 0 then
    for _, row in ipairs(rows) do
      if api_id == row.api_id and consumer_id == row.consumer_id then
        return row
      end
    end
  end

  return nil
end


--- Load the configuration for a plugin entry in the DB.
-- Given an API, a Consumer and a plugin name, retrieve the plugin's
-- configuration if it exists. Results are cached in ngx.dict
-- @param[type=string] api_id ID of the API being proxied.
-- @param[type=string] consumer_id ID of the Consumer making the request (if any).
-- @param[type=stirng] plugin_name Name of the plugin being tested for.
-- @treturn table Plugin retrieved from the cache or database.
local function load_plugin_configuration(api_id, consumer_id, plugin_name)
  local plugin_cache_key = singletons.dao.plugins:cache_key(plugin_name,
                                                            api_id,
                                                            consumer_id)
  local plugin, err = singletons.cache:get(plugin_cache_key, nil,
                                           load_plugin_into_memory,
                                           api_id, consumer_id, plugin_name)
  if err then
    assert(type(err) == "string")
    return err -- forward error out of the coroutine
  end
  if plugin ~= nil and plugin.enabled then
    return plugin.config or {}
  end
end


local function load_one_plugin(self, plugin)
  local api = self.api
  local ctx = self.ctx

  local operations = {}

  local consumer = ctx.authenticated_consumer
  if consumer then
    local consumer_id = consumer.id
    local schema      = plugin.schema

    if schema and not schema.no_consumer then
      if api then
        table_insert(operations, { load_plugin_configuration, api.id, consumer_id, plugin.name })
      end
      table_insert(operations, { load_plugin_configuration, nil, consumer_id, plugin.name })
    end
  end

  -- Search API specific, or global
  if api then
    table_insert(operations, { load_plugin_configuration, api.id, nil, plugin.name })
  end
  table_insert(operations, { load_plugin_configuration, nil, nil, plugin.name })

  local results = parallelize(operations)

  for i = 1, #operations do
    local r = get_result(results, i)

    if r then
      if type(r) == "string" then -- forwarded error from coroutine
        ngx.ctx.delay_response = false
        return responses.send_HTTP_INTERNAL_SERVER_ERROR(r)
      end

      return r
    end
  end
end


local function get_next(self)
  local i = self.i
  local plugin = self.loaded_plugins[i]
  if not plugin then
    return nil
  end
  self.i = i + 1

  -- load the plugin configuration in early phases
  local plugin_conf
  if self.access_or_cert_ctx then
    plugin_conf = load_one_plugin(self, plugin)
    self.ctx.plugins_for_request[plugin.name] = plugin_conf
  else
    plugin_conf = self.ctx.plugins_for_request[plugin.name]
  end

  if plugin_conf then
    return plugin, plugin_conf
  end

  -- no plugin configuration, skip to the next one
  return get_next(self)
end


--- Plugins for request iterator.
-- Iterate over the plugin loaded for a request, stored in
-- `ngx.ctx.plugins_for_request`.
-- @param[type=boolean] access_or_cert_ctx Tells if the context
-- is access_by_lua_block. We don't use `ngx.get_phase()` simply because we can
-- avoid it.
-- @treturn function iterator
local function iter_plugins_for_req(loaded_plugins, access_or_cert_ctx)
  local ctx = ngx.ctx

  local plugins_for_request = ctx.plugins_for_request
  if not plugins_for_request then
    plugins_for_request = {}
    ctx.plugins_for_request = plugins_for_request
  end

  local plugin_iter_state = {
    i                     = 1,
    ctx                   = ctx,
    api                   = ctx.api,
    loaded_plugins        = loaded_plugins,
    access_or_cert_ctx    = access_or_cert_ctx,
  }

  return get_next, plugin_iter_state
end

return iter_plugins_for_req
