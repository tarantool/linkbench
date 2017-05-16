#!/usr/bin/tarantool

-- Clean old data
-- os.execute('rm -rf 51* 0* *.xlog *.snap *.vylog')

box.cfg{
    listen=3301,
    log_level=4,
    log='tarantool.log',
    vinyl_cache = 2*1024*1024*1024
}

require('linkbench')

require('console').start()
