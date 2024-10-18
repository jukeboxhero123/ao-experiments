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
        table.insert(rows, row)
    end
    stmt:reset()
    return rows
end

Handlers.add('upsert', 'Upsert', function (msg)
    local upsertFields = json.decode(msg.Tags.UpsertFields or "{}")

    local stmt = DB:prepare [[
        REPLACE INTO objects (id, name, budget)
        VALUES (:id, :name, :budget);
    ]]

    if not stmt then
        error("Failed to prepare SQL statement: " .. DB:errmsg())
    end

    stmt:bind_names({
        id = upsertFields.Id or uuid(),
        name = upsertFields.Name, budget = upsertFields.Budget
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
        SELECT id, name, budget
        FROM objects
        WHERE (:id IS NULL OR id = :id)
        AND (:name IS NULL OR name = :name) AND (:budget IS NULL OR budget = :budget);;
    ]]

    if not stmt then
        error("Failed to prepare SQL statement: " .. DB:errmsg())
    end

    stmt:bind_names({
        id = queryParams.Id, name = queryParams.Name, budget = queryParams.Budget
    })

    local result = query(stmt)

    stmt:finalize()

    msg.reply({
        Data = json.encode(result),
        Result = 'Get-Result-Success'
    })
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
    local res = DB:exec[[
        CREATE TABLE objects (
            id VARCHAR(255) PRIMARY KEY CHECK(typeof(id) = 'text'), name VARCHAR(255) CHECK(typeof(name) = 'text'), budget INT CHECK(typeof(budget) = 'integer')
        );
    ]]
    print(res)
    isInitialized = true
end

if not isInitialized then
    initialize()
end