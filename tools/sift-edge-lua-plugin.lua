-- Global persistent state to retain the latest values across function calls.
local merged_state = {}

-- Recursively flatten a nested table into a flat dictionary of scalars.
local function flatten(t, prefix, result)
    for k, v in pairs(t) do
        local key = prefix == "" and tostring(k) or (prefix .. "." .. tostring(k))
        
        local val_type = type(v)
        if val_type == "table" then
            -- Recursively flatten nested JSON objects and arrays
            flatten(v, key, result)
        elseif val_type == "number" then
            -- Pin all numbers to double to avoid schema conflicts. 
            result[key] = todouble(tostring(v))
        elseif val_type == "string" then
            -- Extract a number if the string STARTS with one (handles "-83dBm", "0.4", "100%", etc.)
            -- ^     : Start of string
            -- %-?   : Optional minus sign
            -- %d+   : One or more digits
            -- %.?   : Optional decimal point
            -- %d*   : Zero or more trailing digits
            local num_str = v:match("^%-?%d+%.?%d*")
            
            if num_str then
                result[key] = todouble(num_str)
            else
                result[key] = v
            end
            
        elseif val_type == "boolean" then
            result[key] = v 
        end
    end
end

-- Decoder: Parses JSON, merges state, and emits the combined frame.
function decode(identifier, bytes)
    local data = json.decode(bytes)
    
    if type(data) ~= "table" then
        return nil
    end
    
    -- Flatten the newly received data directly into our global merged_state
    flatten(data, "", merged_state)
    
    -- Return the global state directly
    return merged_state
end

function encode(identifier, frames)
    error("This plugin doesn't support encoding")
end