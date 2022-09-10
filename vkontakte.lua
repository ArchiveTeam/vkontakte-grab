dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local item_type = nil
local item_name = nil
local item_value = nil
local item_value_match = nil

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}
local allowed_urls = {}
local item_user = nil

local username = nil

local retry_url = false

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
print('queuing' , item)
  if not target[item] then
    target[item] = true
  end
end

allowed = function(url, parenturl)
  if allowed_urls[url] then
    return true
  end

  local article = string.match(url, "^https?://vk%.com/@([^%?&]+)")
  if article then
    discovered_items["article:" .. article] = true
    return false
  end

  if ids[string.match(url, "[%?&]sig=([^&]+)")] then
    return true
  end

  if string.match(url, "/away%.php") then
    local outlink = string.match(url, "[%?&]to=([^&]+)")
    discovered_outlinks[urlparse.unescape(outlink)] = true
    return true
  end

  if string.match(url, "^https?://[^/]*userapi%.com/.") then
    discovered_items[url] = true
    return false
  end

  for s in string.gmatch(url, "(%-?[0-9]+)") do
    if ids[s] then
      return true
    end
  end

  if string.match(url, "^https?://vk%.com/(.+)$") == username then
    return true
  end

  return false
end

find_item = function(url)
  local value = string.match(url, "^https?://vk%.com/id(%-?[0-9]+)$")
  local type_ = "id"
  local user = nil
  if not value then
    user, value = string.match(url, "^https?://vk%.com/wall(%-?[0-9]+)_([0-9]+)$")
    type_ = "wall"
  end
  if not value and string.match(url, "^https?://[^/]*userapi%.com/.")
    and not allowed(url) then
    value = url
    type_ = "url"
  end
  if value then
    item_type = type_
    item_value = value
    item_value_match = string.gsub(item_value, "%-", "%%%-")
    item_user = user
    item_user_match = nil
    if item_user then
      item_name_new = item_type .. ":" .. item_user .. ":" .. item_value
      item_user_match = string.gsub(item_user, "%-", "%%%-")
    else
      item_name_new = item_type .. ":" .. item_value
    end
    if item_name_new ~= item_name then
      ids = {}
      username = nil
      ids[value] = true
      abortgrab = false
      tries = 0
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  if allowed(url, parent["url"]) then
    if not processed(url) then
      return true
    end
  else
    discovered_outlinks[url] = true
  end

  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function decode_codepoint(newurl)
    newurl = string.gsub(
      newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
      function (s)
        return unicode_codepoint_as_utf8(tonumber(s, 16))
      end
    )
    return newurl
  end

  local function check(newurl)
    newurl = decode_codepoint(newurl)
    local origurl = url
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and allowed(url_, origurl) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function force_queue(newurl)
    allowed_urls[newurl] = true
    check(newurl)
  end

  if allowed(url) and status_code < 300 then
    html = read_file(file)
    if item_type == "id" and string.match(url, "^https?://vk%.com/id%-?[0-9]+$") then
      local newurl = string.match(html, '<link%s+rel="canonical"%s+href="(https://vk.com/[^"]+)"%s*/>')
      username = string.match(newurl, "^https?://vk%.com/(.+)$")
      check(newurl)
      local max_id = 0
      for wall_id in string.gmatch(html, "https?://vk%.com/wall" .. item_value_match .. "_([0-9]+)$") do
        wall_id = tonumber(wall_id)
        if wall_id > max_id then
          max_id = wall_id
        end
      end
      if max_id > 0 then
        for i=0,max_id do
          discover_item(discovered_items, "wall:" .. item_value .. ":" .. tostring(i))
        end
      end
      for data in string.gmatch(html, '<div%s+class="post_image_stories">%s*<img([^>]+)>%s*</div>') do
        local src = string.match(data, 'src="([^"]+)"')
        local data_post_id = string.match(data, 'data%-post%-id="([^"]+)"')
        if src and data_post_id and string.match(data_post_id, "^([0-9]+)") then
          check(src)
        end
      end
    end
    if item_type == "wall" then
      if string.match(html, '"og:image:secure_url"') then
        html = string.gsub(html, '<meta%s+property="og:image"[^>]+>', '')
      end
      if url == "https://vk.com/al_video.php?act=video_box" then
        local data = JSON:decode(html)
        for _, d in pairs(data["payload"]) do
          if d["player"] then
            local params = d["player"]["params"]
            local params_count = 0
            for _, d in pairs(params) do
              params_count = params_count + 1
            end
            if params_count ~= 1 then
              abort_item()
              return {}
            end
            local newurl = params[0]["dash_uni"]
            ids[string.match(newurl, "[%?&]sig=([^&]+)")] = true
            queue(newurl)
          end
        end
      end
      local sig = string.match(url, "[%?&]sig=([^&]+)")
      if sig and ids[sig] then
        local max_bandwidth = 0
        local max_data = nil
        for data in string.gmatch(html, "(<Representation.-</Representation>)") do
          local bandwidth = tonumber(string.match(data, 'bandwidth="([0-9]+)"'))
          if bandwidth > max_bandwidth then
            max_bandwidth = bandwidth
            max_data = data
          end
        end
        if not max_data then
          abort_item()
          return urls
        end
        local baseurl = string.gsub(string.match(data, "<BaseURL>(.-)</BaseURL>"), "&amp;", "&")
        local indexrange_max = string.match(data, 'indexRange="[0-9]+%-([0-9]+)"')
        local newurl = urlparse.absolute(url, baseurl)
        ids[string.match(newurl, "[%?&]sig=([^&]+)")] = true
        check(newurl)
        check(newurl .. "&bytes=0-" .. indexrange_max)
      end
      for video_data in string.gmatch(html, 'onclick="return%s+showVideo%(([^%)]+), event, this%);"') do
        video_data = string.gsub(video_data, "&quot;", '"')
        local video_id, video_list, video_json = string.match(video_data, '"([^"]+)",%s*"([^"]+)",%s*({.+})$')
        if not video_json then
          abort_item()
          return urls
        end
        video_json = JSON:decode(video_json)
        if video_json["addParams"]["post_id"] == item_user .. "_" .. item_value then
          local items_count = 0
          for k, v in pairs(video_json["addParams"]) do
            items_count = items_count + 1
          end
          if items_count > 1 then
            abort_item()
            return urls
          end
          table.insert(urls, {
            url="https://vk.com/al_video.php?act=video_box",
            method="POST",
            body_data=
              "al=1" ..
              "&autoplay=" .. video_json["autoplay"] ..
              "&autoplay_sound=0" .. 
              "&from_autoplay=1" .. 
              "&list=" .. video_list .. 
              "&module=wall" ..
              "&post_id=" .. video_json["addParams"]["post_id"] ..
              "&stretch_vertical=0" .. 
              "&video=" .. video_id,
            headers={
              ["X-Requested-With"]="XMLHttpRequest",
              ["Content-Type"]="application/x-www-form-urlencoded"
            }
          })
          table.insert(urls, {
            url="https://vk.com/al_video.php?act=video_box",
            method="POST",
            body_data=
              "al=1" ..
              "&autoplay=" .. video_json["autoplay"] ..
              "&list=" .. video_list .. 
              "&module=wall" ..
              "&post_id=" .. video_json["addParams"]["post_id"] ..
              "&video=" .. video_id,
            headers={
              ["X-Requested-With"]="XMLHttpRequest",
              ["Content-Type"]="application/x-www-form-urlencoded"
            }
          })
        end
      end
    end
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  find_item(url["url"])
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()

  if killgrab then
    return wget.actions.ABORT
  end

  find_item(url["url"])

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end
  
  if status_code == 200 then
    downloaded[url["url"]] = true
    downloaded[string.gsub(url["url"], "https?://", "http://")] = true
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if (status_code == 0 or status_code >= 400)
    and status_code ~= 404
    and status_code ~= 403 then
    io.stdout:write("Server returned bad response. Sleeping.\n")
    io.stdout:flush()
    local maxtries = 10
    tries = tries + 1
    if tries > maxtries then
      tries = 0
      abort_item()
      return wget.actions.ABORT
    end
    os.execute("sleep " .. math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    ))
    return wget.actions.CONTINUE
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 4
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and JSON:decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        break
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    if tries == maxtries then
      kill_grab()
    end
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["urls-ajsy1kcax4kmzsu"] = discovered_outlinks
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 100 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end

