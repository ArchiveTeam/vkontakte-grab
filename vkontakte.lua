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
local selected_images = {}
local primary_url = nil
local wall_url = nil

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

read_file = function(file, bytes)
  if not bytes then
    bytes = "*all"
  end
  if file then
    local f = assert(io.open(file))
    local data = f:read(bytes)
    f:close()
    if not data then
      data = ""
    end
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

discover_item = function(target, shard, item)
  if string.match(item, "^u?r?l?:?http://.") then
    item = string.gsub(item, "^(u?r?l?:?)http://(.+)", "%1https://%2")
  end
  local temp = string.match(item, "(.-)%s+2x$")
  if temp then
    item = temp
  end
  if not target[shard] then
    target[shard] = {}
  end
  if not target[shard][item] and item ~= item_name then
print("queuing" , item, "to", shard)
    target[shard][item] = true
  end
end

allowed = function(url, parenturl)
  if allowed_urls[url]
    or string.match(url, "^https://vk%.com/al_photos%.php%?act=")
    or string.match(url, "^https://vk%.com/al_video%.php%?act=")
    or string.match(url, "^https://vk%.com/wkview%.php%?act=") then
    return true
  end

  if string.match(url, "^https?://m%.vk%.com/")
    or string.match(url, "[%?&]lang=")
    or string.match(url, "[%?&]offset=")
    or (
      string.match(url, "wall%-?[0-9]+_([0-9]+)") == item_value
      and string.match(url, "%?reply=")
      and not string.match(url, "&thread=")
    )
    or (
      item_type == "id"
      and (
        string.match(url, "/album%-?[0-9]+_[0-9]+")
        or string.match(url, "/video%-?[0-9]+_[0-9]+")
        or string.match(url, "/wall%-?[0-9]+_[0-9]+")
        or string.match(url, "/topic%-?[0-9]+_[0-9]+")
        or string.match(url, "/photo%-?[0-9]+_[0-9]+")
      )
    ) then
    return false
  end

  local article = string.match(url, "^https?://vk%.com/@([^%?&]+)")
  if article then
    discover_item(discovered_items, "", "article:" .. article)
    return false
  end

  if ids[string.match(url, "[%?&]sig=([^&]+)")] then
    return true
  end

  if string.match(url, "/away%.php") then
    local outlink = string.match(url, "[%?&]to=([^&]+)")
    if not outlink then
      return false
    end
    discover_item(discovered_outlinks, "", urlparse.unescape(outlink))
    return true
  end

  if item_name and (
    string.match(url, "^https?://[^/]*userapi%.com/.")
    or string.match(url, "^https?://[^/]*mycdn%.me/.")
  ) then
    discover_item(discovered_items, "images", "url:" .. url)
    return false
  end

  for s in string.gmatch(url, "(%-?[0-9]+)") do
    if ids[s] then
      return true
    end
  end

  for s in string.gmatch(url, "([0-9]+)") do
    if ids[s] then
      return true
    end
  end

  if username and string.match(url, "^https?://vk%.com/(.+)$") == username then
    return true
  end

  return false
end

find_item = function(url)
  local value = string.match(url, "^https?://vk%.com/id(%-?[0-9]+)$")
  if not value then
    value = string.match(url, "^https?://vk%.com/public(%-?[0-9]+)$")
    if value and not string.match(value, "^%-") then
      value = "-" .. value
    end
  end
  local type_ = "id"
  local user = nil
  if not value then
    user, value = string.match(url, "^https?://vk%.com/wall(%-?[0-9]+)_([0-9]+)$")
    type_ = "wall"
  end
  if not value and string.match(url, "^https?://[^/]*userapi%.com/.") then
    value = url
    type_ = "url"
  end
  if value then
    item_type = type_
    item_value = value
    item_value_match = string.gsub(item_value, "%-", "%%%-")
    item_user = user
    item_user_match = nil
    primary_url = url
    if item_user then
      item_name_new = item_type .. ":" .. item_user .. ":" .. item_value
      item_user_match = string.gsub(item_user, "%-", "%%%-")
    else
      item_name_new = item_type .. ":" .. item_value
    end
    if item_name_new ~= item_name then
      ids = {}
      username = nil
      wall_url = nil
      ids[value] = true
      if string.match(value, "^%-.") then
        ids[string.match(value, "^-(.+)$")] = true
      end
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

  if status_code >= 300 and parent["url"] == primary_url then
    return true
  end

  if abortgrab
    or string.match(url, "/[^%?]+%?w=wall%-?[0-9]+_[0-9]+$")
    or string.match(url, "/away%.php%?") then
    return false
  end

  if allowed(url, parent["url"]) then
    if not processed(url) then
      return true
    end
  elseif string.match(url, "^https?://[^/]*userapi%.com/.") then
    discover_item(discovered_items, "images", "url:" .. url)
  elseif not string.match(url, "^https?://m?%.?vk%.com/") then
    discover_item(discovered_outlinks, "", url)
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
    local split_a, split_b = string.match(newurl, "^(https?://.-),(https?://.+)$")
    if split_a and split_b then
      check(split_a)
      check(split_b)
      return nil
    end
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
print(url_)
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

  local function xml_post_request(url, body_data)
    local representation = url .. body_data
    if not processed(representation) then
      print("requesting", url, body_data)
      table.insert(urls, {
        url=url,
        method="POST",
        body_data=body_data,
        headers={
          ["X-Requested-With"]="XMLHttpRequest",
          ["Content-Type"]="application/x-www-form-urlencoded"
        }
      })
      addedtolist[representation] = true
    end
  end

  local function check_sig(url)
    ids[string.match(url, "[%?&]sig=([^&]+)")] = true
    check(url)
  end

  local function process_al_photos(data)
    local found_photos = {}
    for k, v in pairs(data[4]) do
      if ids[string.match(v["id"], "^%-?[0-9]+_([0-9]+)")] then
        local max_size = 0
        local max_url = nil
        for k1, v1 in pairs(v) do
          if type(v1) == "table" and v[k1 .. "src"] and v1[3] > max_size then
            max_size = v1[3]
            max_url = v1[1]
          end
        end
        print('Found max URL ' .. max_url)
        if not max_url then
          io.stdout:write("Could not find photo in al_photos.php data.\n")
          io.stdout:flush()
          abort_item()
          error()
        end
        table.insert(found_photos, {
          url=max_url,
          original=v
        })
      end
    end
    return found_photos
  end

  local function process_video_player(data)
    local params = data["params"]
    local params_count = 0
    for _ in pairs(params) do
      params_count = params_count + 1
    end
    if params_count > 1 then
      io.stdout:write("There should only be one parameter in the video player data.\n")
      io.stdout:flush()
      abort_item()
      error()
    end
    params = params[1]
    --[[local newurl = params["dash_sep"]
    check_sig(newurl)]]
    check(params["author_photo"])
    check(params["jpg"])
    if params["subs"] then
      for _, sub_data in pairs(params["subs"]) do
        check_sig(sub_data["url"])
      end
    end
    local max_pixels = 0
    local max_url = nil
    for k, v in pairs(params) do
      local pixels = tonumber(string.match(k, "^url([0-9]+)$"))
      if pixels and pixels > max_pixels then
        max_pixels = pixels
        max_url = v
      end
      if string.match(k, "^first_frame") then
        check(v)
      end
    end
    if not max_url then
      io.stdout:write("Could not find regular max size video URL.\n")
      io.stdout:flush()
      abort_item()
      error()
    end
    print('Found max URL ' .. max_url)
    check_sig(max_url)
  end

  local function process_al_video(data)
    local found = false
    for _, e in pairs(data) do
      if type(e) == "table" and e["mvData"] and ids[tostring(e["mvData"]["vid"])] then
        check(e["mvData"]["authorPhoto"])
        if e["player"] then
          process_video_player(e["player"])
          found = true
        end
      end
    end
    return found
  end

  local function sorted_params(data)
    local keys = {}
    local params = ""
    for k, v in pairs(data) do
      table.insert(keys, k)
    end
    table.sort(keys)
    for _, k in pairs(keys) do
      if keys[k] then
        if string.len(params) > 0 then
          params = params .. "&"
        end
        params = params .. k .. "=" .. keys[k]
      end
    end
    return params
  end

  local public_part = string.match(url, "^https://vk%.com/public([0-9].+)$")
  if public_part then
    check('https://vk.com/club' .. public_part)
  end

  if allowed(url) and status_code < 300
    and not string.match(url, "^https?://[^/]*userapi%.com/")
    and url ~= "https://vk.com/al_video.php?act=load_playlist_videos" then

    if string.match(url, "[%?&]sig=")
      and not string.match(read_file(file, 20), "[xX][mM][lL]") then
      return urls
    end

    html = read_file(file)

    -- ACCOUNT/GROUP
    if item_type == "id"
      and (
        string.match(url, "^https?://vk%.com/public%-?[0-9]+$")
        or string.match(url, "^https?://vk%.com/id%-?[0-9]+$")
      ) then
      local newurl = string.match(html, '<link%s+rel="canonical"%s+href="(https?://vk%.com/[^"]+)"%s*/>')
      username = string.match(newurl, "^https?://vk%.com/(.+)$")
      check(newurl)
      newurl = string.match(html, '<meta%s+property="og:url"%s+content="([^"]+)"')
      if newurl then
        check(newurl)
      end
      local max_id = 0
      for wall_id in string.gmatch(html, "/wall" .. item_value_match .. "_([0-9]+)") do
        wall_id = tonumber(wall_id)
        if wall_id > max_id then
          max_id = wall_id
        end
      end
      if max_id > 0 then
        for i=0,max_id do
          discover_item(discovered_items, "", "wall:" .. item_value .. ":" .. tostring(i))
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

    -- POST
    if item_type == "wall" then
      if string.match(url, "/[^%?]+%?w=wall%-?[0-9]+_[0-9]+$") then
        return urls
      end

      -- POST DOCUMENTS
      if allowed_urls[url] and string.match(url, "%?extra=") then
        local newurl = string.match(url, "^([^%?]+)")
        allowed_urls[newurl] = true
        check(newurl)
      end
      if string.match(url, "^https?://vk%.com/doc%-?[0-9]+_[0-9]+") then
        check(string.match(url, "^([^%?]+)"))
        for d in string.gmatch(html, "Docs%.initDoc%(({.-})%);") do
          local json = JSON:decode(d)
          local newurl = json["docUrl"]
          allowed_urls[newurl] = true
          check(newurl)
        end
      end
      for d in string.gmatch(html, '(<div%s+class="page_doc_row"%s+id="post_media_lnk[^"]+">.-</div>)') do
        local post_media_lnk = string.match(d, 'post_media_lnk(%-?[0-9]+_[0-9]+)')
        local doc_href = string.match(d, 'href="(/doc[^"]+)"')
        if post_media_lnk == item_user .. "_" .. item_value then
          doc_href = urlparse.absolute(url, doc_href)
          local doc_id = string.match(doc_href, "/doc%-?[0-9]+_([0-9]+)")
          ids[doc_id] = true
          check(doc_href)
          check(string.match(doc_href, "^([^%?]+)"))
        end
      end

      -- POST VIDEO
      if url == "https://vk.com/al_video.php?act=video_box"
        or url == "https://vk.com/al_video.php?act=show" then
        local data = JSON:decode(html)
        local found = false
        for _, d in pairs(data["payload"]) do
          if type(d) == "table" then
            if process_al_video(d) then
              found = true
            end
          end
        end
        if not found and url ~= "https://vk.com/al_video.php?act=show" then
print(html)
          io.stdout:write("Could not find video data in video response.\n")
          io.stdout:flush()
          abort_item()
          error()
        end
        return urls
      end
      local sig = string.match(url, "[%?&]sig=([^&]+)")
      if sig and ids[sig] then
        for adaptation_set in string.gmatch(html, "(<AdaptationSet.-</AdaptationSet>)") do
          local max_bandwidth = 0
          local max_data = nil
          for data in string.gmatch(adaptation_set, "(<Representation.-</Representation>)") do
            local bandwidth = tonumber(string.match(data, 'bandwidth="([0-9]+)"'))
            if bandwidth > max_bandwidth then
              max_bandwidth = bandwidth
              max_data = data
            end
          end
          if not max_data then
            io.stdout:write("No maximum bandwidth found.\n")
            io.stdout:flush()
            abort_item()
            error()
          end
          local baseurl = string.gsub(string.match(max_data, "<BaseURL>(.-)</BaseURL>"), "&amp;", "&")
          local indexrange_max = string.match(max_data, 'indexRange="[0-9]+%-([0-9]+)"')
          local newurl = urlparse.absolute(url, baseurl)
          check_sig(newurl)
          check(newurl .. "&bytes=0-" .. indexrange_max)
        end
      end
      for _, func_name in pairs({"showVideo", "showInlineVideo"}) do
        for video_data in string.gmatch(html, 'onclick="return%s+' .. func_name .. '%(([^%)]+), event, this%);"') do
          video_data = string.gsub(video_data, "&quot;", '"')
          local video_id, video_list, video_json = string.match(video_data, '"([^"]+)",%s*"([^"]+)",%s*({.+})$')
          if not video_json then
            io.stdout:write("No video JSON data found.\n")
            io.stdout:flush()
            abort_item()
            error()
          end
          video_json = JSON:decode(video_json)
          if video_json["addParams"]
            and video_json["addParams"]["post_id"] == item_user .. "_" .. item_value then
            ids[string.match(video_id, "([0-9]+)$")] = true
            local items_count = 0
            for k, v in pairs(video_json["addParams"]) do
              items_count = items_count + 1
            end
            if items_count > 1 then
              io.stdout:write("More than a single addParams parameter found for video.\n")
              io.stdout:flush()
              abort_item()
              error()
            end
            check(urlparse.absolute(url, "/video" .. video_id .. "?list=" .. video_list))
            check(urlparse.absolute(url, "/video" .. video_id))
            for _, module_ in pairs({"wall", "public"}) do
              xml_post_request(
                "https://vk.com/al_video.php?act=video_box",
                "al=1"
                .. "&autoplay=" .. video_json["autoplay"]
                .. "&autoplay_sound=0"
                .. "&from_autoplay=1"
                .. "&list=" .. video_list
                .. "&module=" .. module_
                .. "&post_id=" .. video_json["addParams"]["post_id"]
                .. "&stretch_vertical=0"
                .. "&video=" .. video_id
              )
              xml_post_request(
                "https://vk.com/al_video.php?act=video_box",
                "al=1"
                .. "&autoplay=" .. video_json["autoplay"]
                .. "&list=" .. video_list
                .. "&module=" .. module_
                .. "&post_id=" .. video_json["addParams"]["post_id"]
                .. "&video=" .. video_id
              )
              xml_post_request(
                "https://vk.com/al_video.php?act=show",
                "act=show"
                .. "&al=1"
                .. "&al_ad=0"
                .. "&autoplay=1"
                .. "&expand_player=1"
                .. "&list=" .. video_list
                .. "&load_playlist=1"
                .. "&module=" .. module_
                .. "&playlist_id=post_" .. video_json["addParams"]["post_id"]
                .. "&post_id=" .. video_json["addParams"]["post_id"]
                .. "&video=" .. video_id
                .. "&webcast=0"
              )
            end
            if string.match(url, "[%?&]z=video") then
              local z_data = string.match(html, '%(({"zFields".-})%);')
              local params_data = string.match(html, '%(({"params".-})%);')
              if not z_data or not params_data then
                io.stdout:write("No video data with zFields or params found.\n")
                io.stdout:flush()
                abort_item()
                error()
              end
              local z_data = JSON:decode(z_data)
              local params_data = JSON:decode(params_data)
              if "?z=" .. string.gsub(z_data["zFields"]["z"], "/", "%2F") ~= params_data["params"]["loc"] then
                io.stdout:write("Inconsistency in video data.\n")
                io.stdout:flush()
                abort_item()
                error()
              end
              check(urlparse.absolute(url, params_data["params"]["loc"]))
              local z = video_id .. "%2F" .. video_list .. "%2Fpl_post_" .. item_user .. "_" .. item_value
              xml_post_request(
                "https://vk.com/al_video.php?act=show",
                "_nol=%7B%220%22%3A%22wall" .. item_user .. "_" .. item_value .. "%22%2C%22z%22%3A%22" .. z .. "%22%7D"
                .. "&act=show"
                .. "&al=1"
                .. "&autoplay=0"
                .. "&list=" .. string.match(z_data["zFields"]["z"], "[^/]+$")
                .. "&module="
                .. "&video=" .. string.match(z_data["zFields"]["z"], "^[^/]+")
                .. "&webcast=0"
              )
            end
          end
        end
      end
      if string.match(url, "/video%-?[0-9]+_[0-9]+") then
        local params, data = string.match(html, "ajax%.preload%('al_video%.php',%s*({.-}),%s*(%[.-%])%);[^\\]")
        params = JSON:decode(params)
        params["al"] = "1"
        data = JSON:decode(data)
        xml_post_request(
          "https://vk.com/al_video.php?act=show",
          sorted_params(params)
        )
        xml_post_request(
          "https://vk.com/al_video.php?act=load_playlist_videos",
          "al=" .. params["al"]
          .. "&playlist_raw_id=" .. params["playlist_id"]
          .. "&video_raw_id=" .. params["video"]
        )
        if not process_al_video(data) then
          io.stdout:write("Could not find video data in video response.\n")
          io.stdout:flush()
          abort_item()
          error()
        end
        return urls
      end
      if string.match(url, "^https?://vk%.com/video_ext%.php") then
        local params = string.match(html, "playerParams%s*=%s*({.-});")
        params = JSON:decode(params)
        process_video_player(params)
        return urls
      end

      -- POST PHOTOS
      if url == "https://vk.com/al_photos.php?act=show" then
        local json = JSON:decode(html)
        for _, photolist in pairs(json["payload"]) do
          if type(photolist) == "table" then
            local found_photos = process_al_photos(photolist)
            for _, d in pairs(found_photos) do
              local location = d["original"]["id"] .. "/" .. photolist[1]
              if not string.match(url, "[%?&]z=photo") then
                local newurl = wall_url
                if string.match(newurl, "%?") then
                  newurl = newurl .. "&"
                else
                  newurl = newurl .. "?"
                end
                local zphoto = "z=photo" .. string.gsub(location, "/", "%%2F")
                newurl = newurl .. zphoto
                check(newurl)
                newurl = urlparse.absolute(wall_url, d["original"]["author_href"]) .. "?" .. zphoto
                check(newurl)
                newurl = urlparse.absolute(wall_url, d["original"]["author_href"]) .. "?w=" .. string.match(wall_url, "/(wall%-?[0-9]+_[0-9]+)") .. "&" .. zphoto
                check(d["original"]["author_photo"])
              end
            end
          end
        end
        return urls
      end
      if string.match(url, "/wall%-?[0-9]+_[0-9]+") then
        for image_data in string.gmatch(html, 'showPhoto%(([^%)]+), event%)') do
          image_data = string.gsub(image_data, "&quot;", '"')
          local image_id, wall_id, image_json = string.match(image_data, "'(%-?[0-9]+_[0-9]+)',%s*'(wall%-?[0-9]+_[0-9]+)',%s*({.+})%s*$")
          if not image_json then
            io.stdout:write("No image data found.\n")
            io.stdout:flush()
            abort_item()
            error()
          end
          if wall_id == "wall" .. item_user .. "_" .. item_value then
            ids[string.match(image_id, "([0-9]+)$")] = true
            for _, module_ in pairs({"wall", "public"}) do
              xml_post_request(
                "https://vk.com/al_photos.php?act=show",
                "act=show"
                .. "&al=1"
                .. "&al_ad=0"
                .. "&dmcah="
                .. "&list=" .. wall_id
                .. "&module=" .. module_
                .. "&photo=" .. image_id
              )
            end
            if not string.match(wall_url, "%?") then
              check("https://vk.com/photo" .. image_id)
            end
          end
        end
      end
      if string.match(url, "[%?&]z=photo") then
        local data = string.match(html, '%(({"zFields".-})%)')
        if not data then
          io.stdout:write("No photo data with zFields found.\n")
          io.stdout:flush()
          abort_item()
          error()
        end
        local data = JSON:decode(data)
        local max_y = 0
        local max_image = nil
        for k, v in pairs(data["zOpts"]["temp"]) do
          if type(v) == "table" then
            local list_length = 0
            for _ in pairs(v) do
              list_length = list_length + 1
            end
            if list_length == 3 and v[3] > max_y then
              max_y = v[3]
              max_image = v[1]
            end
          end
        end
        if not max_image then
          io.stdout:write("No maximum size image found.\n")
          io.stdout:flush()
          abort_item()
          error()
        end
        check(max_image)
        selected_images[max_image] = true
        return urls
      end
      if string.match(url, "/photo%-?[0-9]+_[0-9]+") then
        if not string.match(url, "%?") then
          check(url .. "?rev=1")
        end
        local params, data = string.match(html, "ajax%.preload%('al_photos%.php',%s*({.-}),%s*(%[.-%])%);[^\\]")
        params = JSON:decode(params)
        data = JSON:decode(data)
        local found_photos = process_al_photos(data)
        for _, d in pairs(found_photos) do
          check(d["url"])
          check(d["original"]["author_photo"])
        end
        xml_post_request(
          "https://vk.com/al_photos.php?act=show",
          "act=" .. params["act"]
          .. "&al=1"
          .. "&dmcah="
          .. "&list=" .. string.gsub(params["list"], "/", "%%2F")
          .. "&module=" .. params["module"]
          .. "&photo=" .. params["photo"]
        )
        return urls
      end

      -- POST HTML
      if string.match(url, "/wall%-?[0-9]+_[0-9]+")
        and not string.match(wall_url, "%?") then
        local author_data = string.match(html, '<a%s+(class="author"[^>]+)>')
        if not author_data then
          if string.match(html, '<div%s+class="message_page_title">Error</div>')
            and string.match(html, '<div%s+class="message_page_body">%s*Post%s+not%s+found') then
            return urls
          end
          io.stdout:write("No author data found.\n")
          io.stdout:flush()
          abort_grab()
          error()
        end
        local data_from_id = string.match(author_data, 'data%-from%-id="(%-?[0-9]+)"')
        if data_from_id == item_user then
          local author_slug = string.match(author_data, 'href="/([^"/]+)"')
          local wall_id = string.match(url, "/(wall%-?[0-9]+_[0-9]+)")
          check("https://vk.com/" .. author_slug .. "?w=" .. wall_id)
          xml_post_request(
            "https://vk.com/wkview.php?act=show",
            "act=show"
            .. "&al=1"
            .. "&dmcah="
            .. "&loc=" .. author_slug
            .. "&location_owner_id=" .. data_from_id
            .. "&ref=club"
            .. "&w=" .. wall_id
          )
        end
      end
    end

    -- GENERAL
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
  status_code = http_stat["statcode"]
  find_item(url["url"])
  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if not string.match(newloc, "%?reply=")
      and not string.match(newloc, "^https?://away%.vk%.com/")
      and not string.match(newloc, "^https?://login%.vk%.com/%?")
      and not string.match(newloc, "^https?://vk%.com/login%.php%?")
      and not string.match(newloc, "^https?://vk%.com/login%?")
      and not string.match(newloc, "^https?://vk%.com/public")
      and not string.match(newloc, "^https?://[^/]*userapi%.com/")
      and string.match(url["url"], "^https?://[^/]+/([^/%?&;]+)") ~= string.match(newloc, "^https?://[^/]+/([^/%?&;]+)") then
      io.stdout:write("Found odd redirect.\n")
      io.stdout:flush()
      abort_item()
      return false
    end
  end
  if item_type == "wall" and not wall_url then
    local html = read_file(http_stat["local_file"])
    --[[if string.match(html, 'onclick="return%s+show[A-Za-z0-9]+Video%(')
      or string.match(html, "data%-video") then
      io.stdout:write("Videos are not supported yet.\n")
      io.stdout:flush()
      abort_item()
      return false
    end]]
    local data_audio = string.match(html, 'data%-audio="([^"]+)"')
    if data_audio then
      data_audio = string.gsub(data_audio, "&quot;", '"')
      data_audio = JSON:decode(data_audio)
      if data_audio[3] ~= "" then
        io.stdout:write("Audios are not supported yet.\n")
        io.stdout:flush()
        abort_item()
        return false
      end
    end
  end
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

  if not wall_url and string.match(url["url"], "/wall") and status_code == 200 then
    wall_url = url["url"]
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if string.match(url["url"], "^https?://vk%.com/doc%-?[0-9]+_[0-9]+") then
      allowed_urls[newloc] = true
    end
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

  if status_code == 0 or status_code >= 400 then
    io.stdout:write("Server returned bad response. Sleeping.\n")
    io.stdout:flush()
    local maxtries = 5
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
  local function submit_backfeed(items, key, shard)
    local tries = 0
    local maxtries = 4
    local parameters = ""
    if shard ~= "" then
      parameters = "?shard=" .. shard
    end
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key .. parameters,
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
    ["urls-ajsy1kcax4kmzsu"] = discovered_outlinks,
    ["vkontakte-y6v8h27ero5j7v0"] = discovered_items
  }) do
    for shard, urls_data in pairs(data) do
      print("queuing for", string.match(key, "^(.+)%-"), "on shard", shard)
      local items = nil
      local count = 0
      for item, _ in pairs(urls_data) do
        print("found item", item)
        if items == nil then
          items = item
        else
          items = items .. "\0" .. item
        end
        count = count + 1
        if count == 100 then
          submit_backfeed(items, key, shard)
          items = nil
          count = 0
        end
      end
      if items ~= nil then
        submit_backfeed(items, key, shard)
      end
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

