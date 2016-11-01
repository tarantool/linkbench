--call tarantool app.lua
--#!/home/tarantool/src/tarantool

--#!/usr/bin/tarantool
os.execute('rm -rf 51* 0* tarantool.log')

box.cfg{listen=3301, log_level=4, log='tarantool.log', vinyl_cache = 2*1024*1024*1024}


local engine='vinyl'
local type_idx = 'tree'
local threshold_time_warn = 1
local coef_batch_select_limit = 4
local cycle_count_threshhold = 5

box.schema.user.create('linkbench', {password = 'test', if_not_exists = true})
box.schema.user.grant('linkbench', 'execute,read,write', 'universe', nil, {if_not_exists = true})

links = box.schema.space.create('links', {id = 513, if_not_exists = true, engine=engine})
links:create_index('primary', {id = 0, unique = true, type = type_idx, parts={1, 'unsigned', 2, 'unsigned', 3, 'unsigned'}, if_not_exists = true})
--links:create_index('id1_type', {unique = true, type = 'tree', 
--                        parts={1, 'unsigned', 3, 'unsigned', 4, 'scalar', 6,'unsigned', 2, 'unsigned', 7, 'unsigned', 5,'string'}, if_not_exists = true})

links:create_index('id1_type_sh', {id = 1, unique = false, type='tree', parts={1, 'unsigned', 3, 'unsigned' }, if_not_exists = true}) 
links:create_index('id1_type_vis', {id = 2, unique = false, type='tree', parts={1, 'unsigned', 3, 'unsigned', 4, 'scalar'}, if_not_exists = true}) 
links:create_index('time', {id = 4, unique = false, type='tree', parts={6, 'unsigned'}, if_not_exists = true})

--value is taken from LinkStore for usage in getLinkList
DEFAULT_LIMIT = 10000

--box.space.links:format{
--        {name='id1',type='unsigned'},
--        {name='id2',type='unsigned'},
--        {name='link_type',type='unsigned'},
--        {name='visibility',type='boolean'},
--        {name='data', type = 'string'},
--        {name='time', type = 'unsigned'},
--        {name='version', type = 'unsigned'}
--    }
nodes = box.schema.space.create('nodes', {id = 514, if_not_exists = true, engine=engine})
nodes:create_index('primary', {type = 'tree', parts={1, 'unsigned'}, if_not_exists = true})

--box.space.nodes:format{
--    {name='id', type='unsigned'},
--    {name='type', type='unsigned'},
--    {name='version', type='unsigned'},
--    {name='time', type='unsigned'},
--    {name='data', type='string'}
--}

counter_node = table.maxn(nodes)

counts = box.schema.space.create('counts', {id=515, if_not_exists = true, engine=engine})
counts:create_index('primary', {type=type_idx, parts={1, 'unsigned', 2, 'unsigned'}, if_not_exists = true})

--box.space.counts:format{
--    {name='id', type='unsigned'},
--    {name='link_type', type='unsigned'},
--    {name='count', type='unsigned'},
--    {name='version', type='unsigned'},
--    {name='time', type='unsigned'}
--}

box.schema.func.create("cfunc", {language="C", if_not_exists=true})

clock = require('clock')
console = require('console')

function insert_links(list_of_objects)

    --box.begin()
    for i= 1, #list_of_objects, 1
    do
        box.begin()
        
        --updating links space
        links:replace(list_of_objects[i])
        box.commit()
        
    end
    
    --box.commit()
end

function insert_link(object)
    box.begin()

    links:replace(object)


    local count_t = counts:get{object[1], object[3]}
    local current_time = os.time()


    local to_replace =  {object[1], object[3], 1 , 0, current_time}
    --updating count space
    if (object[4]) then
        if (count_t) then
            to_replace[3] = count_t[3] + 1
            to_replace[4] = count_t[4] + 1
        end
    else
        if (count_t) then
            to_replace[3] = count_t[3] - 1
            to_replace[4] = count_t[4] + 1
        end
    end

    counts:replace(to_replace)

    box.commit()

   
end

function delete_link(index, force)
    box.begin()
    local tuple = box.space.links:get(index)
    

    if (tuple) then
        local current_time = os.time()
        local visibility = tuple[4]
        if (force) then
            links:delete(index)
        end

        
        if (visibility) then
            local counts_t = counts:get{index[1], index[3]}
            if (counts_t) then
                local new_count = 0
                if (counts_t[3] > 0) then
                    new_count = counts_t[3] - 1 
                end
                counts:update({index[1], index[3]}, {{'=', 3, new_count}, {'=', 5, current_time}})
            end
        end
        if (visibility and not force) then
            links:update(index, {{'=', 4, false}})
        end
    end

    box.commit()
end

function get_link(ind)
    return links:get{ind[1], ind[2], ind[3]}
end

function multi_get_link(id1, link_type, id2s)

    local selected = links.index.id1_type_sh:select{id1, link_type}
    local set = {}
    for _,tuple in pairs(selected) do
        set[tuple[2]] = tuple
    end
    local result = {}
    for k, id2 in pairs(id2s) do
            result[k] = set[id2]   
        end

    return result
end

function get_link_list(id1, type_link)
    return links.index.id1_type_vis:select({id1,type_link, true},{limit=DEFAULT_LIMIT} )
end

function count_links(id1, type_link)
    local tuple = counts:select{id1, type_link}
    if (tuple[1]) then
        return tuple[1][3]
    else
        return nil
    end
end

function add_counts(list_of_objects)
    for _, object in pairs(list_of_objects) 
    do
        counts:replace(object)
    end
end


function add_bulk_nodes(list_of_objects)
    local result = {}

    box.begin()
    for i, object in pairs(list_of_objects)
    do
        counter_node = counter_node  + 1
        object[1] = counter_node
        nodes:replace(object)

        result[i] = counter_node
    end

    box.commit()
    return result
end

function get_node(id, type_)

    local node = nodes:get{id}
    if (not node) then 
        return nil
    end

    if (node[2] ~= type_) then
        return nil
    else
        return node
    end
end

function update_node(node)
    local res = false
    box.begin()
    local check = nodes:get{node[1]}
    if (check) then
        nodes:replace(node)
        res = true
    end
    box.commit()
    
    return res
end

function delete_node(id, type_)

    box.begin()

    local node = nodes:get{id}
    local res = true
    if (not node or node[2] ~= type_) then
        res = false
    else 
        nodes:delete{id}
    end

    box.commit()

    return res
end


function print_table(t)
    if (not t) then
        print ("nil")
        return
    end
    for _, v in pairs(t) do
        print(v)
    end
end

--console.start()
