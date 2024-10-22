local sqlite3 = require("lsqlite3")
local isInitialized = isInitialized or false
local crypto = require('.crypto')
local json = require('json')

DB = DB or sqlite3.open_memory()

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
        -- print(row)
        table.insert(rows, row)
    end
    stmt:reset()
    return rows
end

function getEdge(queryParams)
    local stmt = DB:prepare [[
        SELECT id, toId, toIdProcess, fromId, fromIdProcess, label
        FROM edges
        WHERE (:id IS NULL OR id = :id)
        AND (:label IS NULL OR label = :label)
        AND (:fromId IS NULL OR fromId = :fromId)
        AND (:fromIdProcess IS NULL OR fromIdProcess = :fromIdProcess)
        AND (:toId IS NULL OR toId = :toId)
        AND (:toIdProcess IS NULL OR toIdProcess = :toIdProcess);
    ]]

    if not stmt then
        error("Failed to prepare SQL statement: " .. DB:errmsg())
    end
    print(queryParams)
    stmt:bind_names({
        id = queryParams.Id or nil,
        toId = queryParams.ToId or nil,
        toIdProcess = queryParams.ToIdProcess or nil,
        fromId = queryParams.FromId or nil,
        fromIdProcess = queryParams.FromIdProcess or nil,
        label = queryParams.Label or nil,
    })

    local result = query(stmt)
    
    stmt:finalize()

    return result
end


local Direction = {
    IN = "In",
    OUT = "Out"
}

local function isValidEdge(newEdge)
    -- Validate To Node
    Send({ Target = newEdge.ToIdProcess, Action = 'CheckValidEdge', Direction = Direction.IN, Label = newEdge.Label, ConnectedType = newEdge.FromIdProcess })
    local toNodeResult = Receive({ From = newEdge.ToIdProcess }) --, Action = "Check-Valid-Edge-Result" })
    print(toNodeResult)
    if toNodeResult.Data == "Fail" then
        return { isValid = false, reason = toNodeResult.Reason }
    end

    -- Validate From Node
    Send({ Target = newEdge.FromIdProcess, Action = 'CheckValidEdge', Direction = Direction.OUT, Label = newEdge.Label, ConnectedType = newEdge.ToIdProcess })
    local fromNodeResult = Receive({ From = newEdge.FromIdProcess }) --, Action = "Check-Valid-Edge-Result" })
    if fromNodeResult.Data == "Fail" then
        return { isValid = false, reason = fromNodeResult.Reason }
    end

    return { isValid = true }
end

Handlers.add('upsert', 'Upsert', function (msg)
    local upsertFields = json.decode(msg.Tags.UpsertFields or "{}")
    
    local isValidResult = isValidEdge(upsertFields)
    print(isValidResult)
    if not isValidResult.isValid then
        msg.reply({ Action = 'Upsert-Edge-Result', Data = "Fail", Reason = isValidResult.reason })
        return
    end

    local stmt = DB:prepare [[
        REPLACE INTO edges (id, toId, toIdProcess, fromId, fromIdProcess, label)
        VALUES (:id ,:toId, :toIdProcess, :fromId, :fromIdProcess, :label);
    ]]

    print('Upserting...' .. upsertFields.Label)

    if not stmt then
        error("Failed to prepare SQL statement: " .. DB:errmsg())
    end

    stmt:bind_names({
        id = upsertFields.Id or uuid(),
        toId = upsertFields.ToId,
        toIdProcess = upsertFields.ToIdProcess,
        fromId = upsertFields.FromId,
        fromIdProcess = upsertFields.FromIdProcess,
        label = upsertFields.Label,
    })

    local result = stmt:step()
    stmt:reset()

    print(result)
    if result == sqlite3.DONE then
        print('SUCCESS')
        msg.reply({ Action = 'Upsert-Edge-Result', Data = "Success" })
    else
        print('FAIL')
        msg.reply({ Action = 'Upsert-Edge-Result', Data = "Fail" })
    end
end)

Handlers.add('get', 'Get', function (msg)
    local result = getEdge(json.decode(msg.Tags.QueryParams or "{}"))
    print(result)
    msg.reply({
        Data = json.encode(result),
        Result = 'Get-Result-Success'
    })
end)

Handlers.add('test', 'Test', function (msg)
    Send({ Target=ao.id, Action="Get"})
    local test = Receive({ Data='Reply'})
    print(test)
end)

function initialize()
    print('creating a table')
    local res = DB:exec[[
        CREATE TABLE edges (
            id VARCHAR(255) PRIMARY KEY CHECK(typeof(id) = 'text'),
            toId VARCHAR(255) CHECK(typeof(toId) = 'text'),
            toIdProcess VARCHAR(255) CHECK(typeof(toIdProcess) = 'text'),
            fromId VARCHAR(255) CHECK(typeof(fromId) = 'text'),
            fromIdProcess VARCHAR(255) CHECK(typeof(fromIdProcess) = 'text'),
            label VARCHAR(255) CHECK(typeof(label) = 'text')
        );
    ]]
    print(res)
    isInitialized = true
end

Handlers.add('reset', 'Reset', function (msg)
    DB:exec[[
        DROP TABLE edges;
    ]]
    initialize()
end)

if not isInitialized then
    initialize()
end

