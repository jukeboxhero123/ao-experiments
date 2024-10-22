local storeSourceCode = storeSourceCode or [=[
    local sqlite3 = require("lsqlite3")
    local isInitialized = isInitialized or false
    local crypto = require('.crypto')
    local json = require('json')
    
    DB = DB or sqlite3.open_memory()
    
    validEdgeWhitelist = validEdgeWhitelist or {}
    
    local function uuid()
        local template = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'
        return string.gsub(template, '[xy]', function(c)
            local v = (c == 'x') and math.random(0, 0xf) or math.random(8, 0xb)
            return string.format('%x', v)
        end)
    end
    
    function query(stmt)
        local rows = {}
        for row in stmt:nrows() do
            table.insert(rows, row)
        end
        stmt:reset()
        return rows
    end
    
    Handlers.add('upsert', 'Upsert', function (msg)
        local upsertFields = json.decode(msg.Tags.UpsertFields or "{}")
    
        local stmt = DB:prepare [[
            REPLACE INTO objects (id, COLUMN_LIST) 
            VALUES (:id, PARAM_LIST);
        ]]
    
        if not stmt then
            error("Failed to prepare SQL statement: " .. DB:errmsg())
        end
    
        stmt:bind_names({
            id = upsertFields.Id or uuid(),
            BIND_PARAMS_UPSERT
        })
    
        local result = stmt:step()
        stmt:reset()
    
        if result == sqlite3.DONE then
            msg.reply({ Result = "Upsert-Success" })
        else
            msg.reply({ Result = "Upsert-Fail" })
        end
    end)
    
    Handlers.add('get', 'Get', function (msg)
        local queryParams = json.decode(msg.Tags.QueryParams or "{}")
    
        local stmt = DB:prepare [[
            SELECT id, COLUMN_LIST
            FROM objects
            WHERE (:id IS NULL OR id = :id)
            SQL_QUERY;
        ]]
    
        if not stmt then
            error("Failed to prepare SQL statement: " .. DB:errmsg())
        end
    
        stmt:bind_names({
            id = queryParams.Id, BIND_PARAMS_QUERY
        })
    
        local result = query(stmt)
        
        stmt:finalize()
    
        msg.reply({
            Data = json.encode(result),
            Result = 'Get-Result-Success'
        })
    end)
    
    local Direction = {
        IN = "In",
        OUT = "Out"
    }
    
    Handlers.add('addValidEdge', 'AddValidEdge', function (msg)
        if msg.Direction ~= nil and msg.Label ~= nil and msg.ConnectedType ~= nil then
            table.insert(validEdgeWhitelist, { direction = msg.Direction, label = msg.Label, connectedType = msg.ConnectedType })
            msg.reply({ Action = "Add-Valid-Edge-Result", Data = "Success" })
        else
            msg.reply({ Action = "Add-Valid-Edge-Result", Data = "Fail" })
        end
    end)
    
    Handlers.add('checkValidEdge', 'CheckValidEdge', function (msg)
        local direction = msg.Direction
        local label = msg.Label
        local connectedType = msg.ConnectedType
    
        -- Valid if whitelist is empty
        if #validEdgeWhitelist == 0 then
            msg.reply({ Action = "Check-Valid-Edge-Result", Data = "Success" })
            return
        end
    
        for index, edge in ipairs(validEdgeWhitelist) do
            if direction == edge.direction and label == edge.label and connectedType == edge.connectedType then
                msg.reply({ Action = "Check-Valid-Edge-Result", Data = "Success" })
                return
            end
        end
        msg.reply({ Action = "Check-Valid-Edge-Result", Data = "Fail", Reason = label .. " edge connecting to a " .. connectedType .. " node is not allowed to come " .. direction .. " a object of type " .. ao.id .. "."  })
    end)
    
    Handlers.add('getManyById', 'GetManyById', function (msg)
        local ids = json.decode(msg.Tags.ObjectIds or "[]")
    
        local formattedIds = {}
    
        -- Iterate over the IDs and format each one
        for _, id in ipairs(ids) do
            table.insert(formattedIds, string.format('"%s"', id))
        end
    
        -- Join the formatted IDs into a single string
        local idList = table.concat(formattedIds, ", ")
    
        -- Construct the final SQL query
        local sqlQuery = string.format('SELECT * FROM objects WHERE id IN (%s);', idList)
    
        local stmt = DB:prepare(sqlQuery)
    
        if not stmt then
            print("Failed to prepare SQL statement: " .. DB:errmsg())
        end
    
        local result = query(stmt)
        
        stmt:finalize()
    
        msg.reply({
            Data = json.encode(result),
            Result = 'Get-Many-Result-Success'
        })
    end)
    
    function initialize()
        DB:exec[[
            CREATE TABLE objects (
                id VARCHAR(255) PRIMARY KEY CHECK(typeof(id) = 'text'), COLUMN_DEF
            );
        ]]
        isInitialized = true
    end
        
    if not isInitialized then
        initialize()
    end 
]=]

json = require('json')
edgeProcessId = edgeProcessId or "ADt9HmqmNEwfUNHNNyS9t8KR_hG9m5xV7vXloC30cOk"
sqliteModuleId = sqliteModuleId or "aGVVWHldKA7GBlI_w7Qp_agO6aKjCoOTPA1G2OlluXE"
schemaTable = schemaTable or {}

local function capitalizeFirstLetter(str)
    if str == nil or str == "" then
        return str  -- Return nil or empty string unchanged
    end
    
    return str:sub(1, 1):upper() .. str:sub(2)  -- Capitalize first letter and append the rest
end

local function getSQLType(value, name)
    if value == "string" then
        return "VARCHAR(255) CHECK(typeof(" .. name .. ") = 'text')"
    elseif value == "number" then
        return "INT CHECK(typeof(" .. name .. ") = 'integer')"
    else
        return "TEXT CHECK(typeof(" .. name .. ") = 'text')"
    end
end

local function getFieldsSummary(fields)
    local summary = ""
    for index, value in ipairs(fields) do
        summary = summary .. value.type .. " " .. capitalizeFirstLetter(value.name)
        if index < #fields then
            summary = summary .. ", "
        end
    end
    return summary
end

local function getSourceCode(fields)
    -- COLUMN_LIST: toId, fromId, label
    -- PARAM_LIST: :toId, :fromId, :label
    -- BIND_PARAMS: toId = msg.ToId, fromId = msg.FromId, label = msg.Label,
    -- SQL_QUERY: AND (:label IS NULL OR label = :label) AND (:fromId IS NULL OR fromId = :fromId) AND (:toId IS NULL OR toId = :toId);
    -- COLUMN_DEF: toId VARCHAR(255), fromId VARCHAR(255), label VARCHAR(255)
    local columnList = ""
    local paramList = ""
    local bindParams = ""
    local bindParamsQuery = ""
    local sqlQuery = ""
    local columnDef = ""

    for index, value in ipairs(fields) do
        columnList = columnList .. value.name
        if index < #fields then
            columnList = columnList .. ", "
        end
    
        paramList = paramList .. ":" .. value.name
        if index < #fields then
            paramList = paramList .. ", "
        end

        bindParams = bindParams .. value.name .. " = upsertFields." .. capitalizeFirstLetter(value.name)
        if index < #fields then
            bindParams = bindParams .. ", "
        end

        bindParamsQuery = bindParamsQuery .. value.name .. " = queryParams." .. capitalizeFirstLetter(value.name)
        if index < #fields then
            bindParamsQuery = bindParamsQuery .. ", "
        end

        sqlQuery = sqlQuery .. "AND (:" .. value.name .. " IS NULL OR " .. value.name .. " = :" .. value.name .. ")"
        if index < #fields then
            sqlQuery = sqlQuery .. " "
        else
            sqlQuery = sqlQuery .. ";"
        end

        columnDef = columnDef .. value.name .. " " .. getSQLType(value.type, value.name)
        if index < #fields then
            columnDef = columnDef .. ", "
        end
    end

    local newSourceCode = string.gsub(storeSourceCode, "COLUMN_LIST", columnList)
    newSourceCode = string.gsub(newSourceCode, "PARAM_LIST", paramList)
    newSourceCode = string.gsub(newSourceCode, "BIND_PARAMS_UPSERT", bindParams)
    newSourceCode = string.gsub(newSourceCode, "BIND_PARAMS_QUERY", bindParamsQuery)
    newSourceCode = string.gsub(newSourceCode, "SQL_QUERY", sqlQuery)
    newSourceCode = string.gsub(newSourceCode, "COLUMN_DEF", columnDef)

    return newSourceCode
end


local function validateObjectProcessId(msg)
    local objectProcessId = msg.Tags["Object-Process-Id"]
    if objectProcessId == nil or schemaTable[objectProcessId] == nil then
        return nil
    else
        return objectProcessId
    end
end

-- register schema (new schema actor)

Handlers.add('clearSchemas', 'ClearSchemas', function (msg)
    schemaTable = {}
end)

Handlers.add('updateSchema', 'UpdateSchema', function (msg)
    print('Updating Schema')

    local updatedStoreId = validateObjectProcessId(msg)

    -- Get Process Id
    print("Process: " .. updatedStoreId)

    local sourceCode = getSourceCode(json.decode(schemaTable[updatedStoreId].fieldsRaw))

    Send({Target = updatedStoreId, Action = "Eval", Data = sourceCode})

    -- TODO: Figure out if there's a way to wait for Eval actions
    -- local msg = Receive(function(m)
    --     return m.Tags['From-Process'] == newStoreId
    -- end)
    -- print(msg);
end)

Handlers.add('registerSchema', 'RegisterSchema', function (msg)
    print('Registering New Schema')
    local Tags = { Authority = ao.authorities[1] }

    Spawn(sqliteModuleId, {
        Data = "",
        Tags = Tags,
    })
    local newStoreId = Receive({Action = "Spawned"}).Process

    -- Get Process Id
    print("Process: " .. newStoreId)

    local schema = json.decode(msg.Data)

    local sourceCode = getSourceCode(schema.fields)

    schemaTable[newStoreId] = { Name = schema.name, Description = schema.description, Fields = getFieldsSummary(schema.fields), fieldsRaw = json.encode(schema.fields), ProcessId = newStoreId }

    Send({Target = newStoreId, Action = "Eval", Data = sourceCode})

    -- TODO: Figure out if there's a way to wait for Eval actions
    -- local msg = Receive(function(m)
    --     return m.Tags['From-Process'] == newStoreId
    -- end)
    -- print(msg);
end)

-- query (list) schemas
Handlers.add('listSchemas', 'ListSchemas', function (msg)
    print(schemaTable)
    msg.reply({
        Data = json.encode(schemaTable),
    })
end)
-- create new object
Handlers.add('addObject', 'AddObject', function (msg)
    local objectProcessId = validateObjectProcessId(msg)
    if objectProcessId ~= nil then
        Send({ Target = objectProcessId, Action = 'Upsert', UpsertFields = msg.Data })
        local result = Receive({ From = objectProcessId }) -- Need to be more specific about what type of message is being received
        print(result)
        if result.Tags.Result == "Upsert-Success" then
            msg.reply({ Result = "Sucess" })
        else
            msg.reply({ Result = "Fail" })
        end
    else
        msg.reply({
            Action = 'Upsert-Error',
            ['Message-Id'] = msg.Id,
            Error = 'Object-Process-Id is not a valid registered schema actor'
        })
    end
    
end)

-- create new edge
-- TODO: change this to just be the json encoded Data field
Handlers.add('addEdge', 'AddEdge', function (msg) 
    local upsertFields = {
        ["FromId"] = msg.Tags.FromId,
        ["FromIdProcess"] = msg.Tags.FromIdProcess,
        ["ToId"] = msg.Tags.ToId,
        ["ToIdProcess"] = msg.Tags.ToIdProcess,
        ["Label"] = msg.Tags.Label
    }
    Send({ Target = edgeProcessId, Action = 'Upsert', UpsertFields = json.encode(upsertFields) })
    local result = Receive({ From = edgeProcessId, Action = 'Upsert-Edge-Result' }) -- Need to be more specific about what type of message is being received
    print(result)
    if result.Data == "Success" then
        msg.reply({ Action = "Add-Edge-Result", Data = "Sucess" })
    else
        msg.reply({ Action = "Add-Edge-Result", Data = "Fail" })
    end
end)

-- query first degree connections for an object
local function queryEdges(queryParams)
    Send({ Target = edgeProcessId, Action = 'Get', QueryParams = queryParams }) 
    local result = Receive({ From = edgeProcessId }) -- Need to be more specific about what type of message is being received
    local edges = json.decode(result.Data)
    return edges
end

local function queryObject(objectProcessId, objectId)
    Send({ Target = objectProcessId, Action = 'Get' }) 
    local result = Receive({ From = objectProcessId }) -- Need to be more specific about what type of message is being received
    local objects = json.decode(result.Data)
    return objects
end

local function queryManyObject(objectProcessId, objectIds)
    Send({ Target = objectProcessId, Action = 'GetManyById', ObjectIds = json.encode(objectIds) }) 
    local result = Receive({ From = objectProcessId }) -- Need to be more specific about what type of message is being received
    print(result)
    local objects = json.decode(result.Data)
    return objects
end

local Direction = {
    IN = "In",
    OUT = "Out"
}

-- We need to translate query params of GetConnections handler to the query params of the edges store

-- What are queryParams of GetConnections Handler?
--     Object ID we care to branch out from
--         Object-Id
--     (Optional but more difficult so let's say it's required for now) Directionality of a interested connections
--         Direction ("In" or "Out")
--     (Optional) Label
--         Label

Handlers.add('getConnections', 'GetConnections', function (msg)
    local edgeQueryParams = { Label = msg.Label }
    if Direction.IN == msg.Direction then
        edgeQueryParams['ToId'] = msg['Object-Id']
    else 
        edgeQueryParams['FromId'] = msg['Object-Id']
    end
    local edges = queryEdges(json.encode(edgeQueryParams))
    print(edges)
    local objectIdsByProcess = {}
    for index, value in ipairs(edges) do
        if Direction.IN == msg.Direction then
            if objectIdsByProcess[value.fromIdProcess] then
                table.insert(objectIdsByProcess[value.fromIdProcess], value.fromId)
            else
                objectIdsByProcess[value.fromIdProcess] = { value.fromId }
            end
        else 
            if objectIdsByProcess[value.toIdProcess] then
                table.insert(objectIdsByProcess[value.toIdProcess], value.toId)
            else
                objectIdsByProcess[value.toIdProcess] = { value.toId }
            end
        end
    end
    print(objectIdsByProcess)
    local objects = {}
    for processId, objectIds in pairs(objectIdsByProcess) do
        local result = queryManyObject(processId, objectIds)
        for i = 1, #result do
            table.insert(objects, result[i])
        end
    end
    print(objects)
end)

