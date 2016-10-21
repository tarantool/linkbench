#!/usr/bin/tarantool
os.execute('rm -rf 0* 1* 2*  tarantool.log')

box.cfg{listen=3301, log_level=4, logger='tarantool.log', slab_alloc_minimal=24, slab_alloc_factor=1.2, slab_alloc_arena = 5, vinyl = {memory_limit=2*1024*1024*1024}}


local engine='memtx'
local type_idx = 'tree'
local threshold_time_warn = 0.1
local coef_batch_select_limit = 4
local cycle_count_threshhold = 5

box.schema.user.create('linkbench', {password = 'test', if_not_exists = true})
box.schema.user.grant('linkbench', 'execute,read,write', 'universe', nil, {if_not_exists = true})

links = box.schema.space.create('links', {id = 0, if_not_exists = true, engine=engine})
links:create_index('primary', {unique = true, type = type_idx, parts={1, 'unsigned', 2, 'unsigned', 3, 'unsigned'}, if_not_exists = true})
--links:create_index('id1_type', {unique = true, type = 'tree', 
--                        parts={1, 'unsigned', 3, 'unsigned', 4, 'scalar', 6,'unsigned', 2, 'unsigned', 7, 'unsigned', 5,'string'}, if_not_exists = true})

links:create_index('id1_type_sh', {unique = false, type='tree', parts={1, 'unsigned', 3, 'unsigned' }, if_not_exists = true}) 
links:create_index('id1_type_vis', {unique = false, type='tree', parts={1, 'unsigned', 3, 'unsigned', 4, 'scalar'}, if_not_exists = true}) 
links:create_index('time', {unique = false, type='tree', parts={6, 'unsigned'}, if_not_exists = true})

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
nodes = box.schema.space.create('nodes', {id = 1, if_not_exists = true, engine=engine})
nodes:create_index('primary', {type = 'tree', parts={1, 'unsigned'}, if_not_exists = true})

--box.space.nodes:format{
--    {name='id', type='unsigned'},
--    {name='type', type='unsigned'},
--    {name='version', type='unsigned'},
--    {name='time', type='unsigned'},
--    {name='data', type='string'}
--}

counts = box.schema.space.create('counts', {id=2, if_not_exists = true, engine=engine})
counts:create_index('primary', {type=type_idx, parts={1, 'unsigned', 2, 'unsigned'}, if_not_exists = true})

--box.space.counts:format{
--    {name='id', type='unsigned'},
--    {name='link_type', type='unsigned'},
--    {name='count', type='unsigned'},
--    {name='version', type='unsigned'},
--    {name='time', type='unsigned'}
--}


log = require('log')
clock = require('clock')
--console = require('console')

function insert_links(list_of_objects)

    box.begin()
    for i= 1, #list_of_objects, 1
    do
                --box.begin()
        
        --updating links space
        links:replace(list_of_objects[i])
                --box.commit()
        
    end
    
    box.commit()
end

function insert_link(object)
   -- t = links:get{object[1], object[2], object[3]}
        
    log.info("add link " .. object[1] .. '.' .. object[2] .. '.' .. object[3])
    box.begin()

    local start = clock.time()
    links:replace(object)

    local end1 = clock.time()

    local count_t = counts:get{object[1], object[3]}
    local lcurrent_time = os.time()


    local end2 = clock.time()
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
    local end3 = clock.time()

    box.commit()

    log.info("add link finished " .. object[1] .. '.' .. object[2] .. '.' .. object[3])
    if (end3 - start > threshold_time_warn) then
        log.warn("time for addlink = " .. end3 - start)
        log.warn("time links replace= " .. end1 - start)
        log.warn("time get= " .. end2 - end1)
        log.warn("time count replace= " .. end3 - end2)
    end
   
end

function delete_link(index, force)
    --box.begin()
    local start = clock.time()

    local tuple = box.space.links:get(index)
    
    local end1 = clock.time()
    local end2 = clock.time()
    local end3 = clock.time()

    if (tuple) then
        current_time = os.time()
        visibility = tuple[4]
        if (force) then
            links:delete(index)
        end

        end2 = clock.time()
        
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
        end3 = clock.time()
        if (visibility and not force) then
            links:update(index, {{'=', 4, false}})
        end
    end
    local end4 = clock.time()
    if (end4 - start > threshold_time_warn) then
        log.warn("time for deleteLink= " .. end4 - start)
        log.warn("time links get= " .. end1 - start)
        log.warn("time links possible1 delete= " .. end2 - end1)
        log.warn("time counts get + update = " .. end3 - end2)
        log.warn("time links possible2 update = " .. end4 - end3)
    end

    --box.commit()
end

function get_link(ind)
    return links:get{ind[1], ind[2], ind[3]}
end

function multi_get_link(id1, link_type, id2s)
    local start = clock.time()

    local selected = links.index.id1_type_sh:select{id1, link_type}
    local set = {}
    for _,tuple in pairs(selected) do
        set[tuple[2]] = tuple
    end
    local result = {}
    for k, id2 in pairs(id2s) do
            result[k] = set[id2]   
        end

    local end1 = clock.time() 
    if (end1 - start > threshold_time_warn) then

        log.warn("multigetlink too long " .. id1 .. '.' .. link_type)
        log.warn('time ' .. end1 - start)
        log.warn('len= ' .. #result)
    end    
    return result
end


function get_link_list(id1, type_link)
    --local selected = links.index.id1_type_sh:select({id1,type_link})    
    --local result = {}
    --local c = 1
    --
    --for _, t in pairs(selected) do
    --    if (t[4]) then
    --        result[c] = t
    --        c = c + 1
    --        if (c >= DEFAULT_LIMIT) then
    --            break
    --        end
    --    end
    --end
    --return result

    log.info('getlinklist started ' .. id1 .. '.' .. type_link)
    local start = clock.time()
    local result = links.index.id1_type_vis:select({id1,type_link, true},{limit=DEFAULT_LIMIT} )
    local end1 = clock.time()
    log.info('getlinklist finished ' .. id1 .. '.' .. type_link)

    if (end1 - start > threshold_time_warn) then
        log.warn("getlinklist too long " .. id1 .. '.' .. type_link)
        log.warn('time ' .. end1 - start)
        log.warn('len= ' .. #result)
    end    
    return result
end

function get_link_list_time_bound(id1, type_link, low_bound, high_bound, offset, limit)
    local start = clock.time()

    local batch_size = coef_batch_select_limit * limit

    local tuples = {}

    local c = 0 -- how much we missed
    local i = 1 -- counter of added
    local selected = {}
    local cur_offset = 0


    local debug_c = 0
    repeat
        
        if (debug_c == cycle_count_threshhold) then
            tuples = links.index.id1_type_vis:select({id1, type_link, true}, { offset=cur_offset})
            debug_c = debug_c + 1
        else
            tuples = links.index.id1_type_vis:select({id1, type_link, true}, { limit=batch_size, offset=cur_offset})
            cur_offset = cur_offset + batch_size
            debug_c = debug_c + 1

        end

        for _, tuple in pairs(tuples)
            do
                if (tuple[6] <= high_bound and tuple[6] >= low_bound) then
                    if (c >= offset) then  
                        selected[i] = tuple
                        i = i + 1
                        if (i > limit) then
                            break
                        end
                    end
                    c = c + 1
                end
        end
        
    until(i >= limit or #tuples < batch_size or debug_c > cycle_count_threshhold) 
    
    table.sort(selected, function(a,b) return a[6] > b[6] end)
    local end2 = clock.time() 
    if (end2 - start > threshold_time_warn) then
        log.warn("---------------------")
        log.warn("getlinklistTime too long " .. id1 .. '.' .. type_link)
        log.warn("low_bound= " .. low_bound)
        log.warn("high_bound= " .. high_bound)
        log.warn("offset= " .. offset)
        log.warn("limit= " .. limit)
        log.warn("loops= " .. debug_c)
        log.warn('time ' .. end2 - start)
        log.warn("len first selection= " .. #tuples)
        log.warn('len second select= ' .. #selected)
        log.warn("---------------------")
    end    
    return selected
end

function count_links(id1, type_link)
    local start = clock.time()

    local tuple = counts:select{id1, type_link}
    if (tuple[1]) then
        return tuple[1][3]
    else
        return nil
    end
    local end1 = clock.time()
    if (end1 - start > threshold_time_warn) then
        log.warn("time for countLink= " .. end1 - start)
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
        tuple = nodes:auto_increment(object)
        
        result[i] = tuple[1]
    end

    box.commit()
    return result
end

function get_node(id, type_)
    local start = clock.time()

    local node = nodes:get{id}
    if (not node) then 
        return nil
    end

    if (node[2] ~= type_) then
        return nil
    else
        return node
    end
    local end1 = clock.time()
    if (end1 - start > threshold_time_warn) then
        log.warn("time for getNode= " .. end1 - start)
    end 
end

function update_node(node)
    local start = clock.time()
    local res = false
    box.begin()
    local check = nodes:get{node[1]}
    if (check) then
        nodes:replace(node)
        res = true
    end
    box.commit()
    
    local end1 = clock.time()
    if (end1 - start > threshold_time_warn) then
        log.warn("time for updateNode= " .. end1 - start)
    end 

    return res
end

function delete_node(id, type_)
    local start = clock.time()

    box.begin()

    local node = nodes:get{id}
    local res = true
    if (not node or node[2] ~= type_) then
        res = false
    else 
        nodes:delete{id}
    end

    box.commit()

    local end1 = clock.time()
    if (end1 - start > threshold_time_warn) then
        log.warn("time for deleteNode= " .. end1 - start)
    end 
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

--t = get_link_list_time_bound(15, 4, 2990, 3001, 0, 10)
--print_table(t)
--os.exit(0)

