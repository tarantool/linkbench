#include "module.h"

#include <time.h>

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#define MP_SOURCE 1
#include "msgpuck.h"

const uint32_t SPACE_ID = 513;
const uint32_t INDEX_ID = 2;
const int time_field_num = 5;
const float TIME_THRESHOLD = 1;

int cmp(const void* e1, const void* e2);

int cfunc(box_function_ctx_t *ctx, const char *args, const char *args_end){
    uint32_t arg_count = mp_decode_array(&args);
    if (arg_count < 6) {
        return box_error_set(__FILE__, __LINE__, ER_PROC_C, "%s", "invalid argument count");
    }

    uint64_t id1 = mp_decode_uint(&args);
    uint64_t type = mp_decode_uint(&args);
    uint64_t low = mp_decode_uint(&args);
    uint64_t high = mp_decode_uint(&args);
    uint64_t offset = mp_decode_uint(&args);
    uint64_t limit = mp_decode_uint(&args);
    

    char buf[1024];
    char* end = buf;
    end = mp_encode_array(end, 3);
    end = mp_encode_uint(end, id1);
    end = mp_encode_uint(end, type);
    end = mp_encode_bool(end, true);

    box_tuple_t* tmp;
    box_txn_begin();

    box_iterator_t* it = box_index_iterator(SPACE_ID, INDEX_ID, ITER_EQ, buf, end); 
    if (it == NULL){
        printf("iterator null \n");
        printf("%s\n", box_error_message(box_error_last()));
        exit(-1);
    }
    box_tuple_t** result = calloc(limit, sizeof(*result));
    int missed = 0;
    int count = 0;
    

    while (!box_iterator_next(it, &tmp)){
        if (tmp == NULL){
            break;
        }
        const char* time_msg = box_tuple_field(tmp, time_field_num);
        uint64_t time_val = mp_decode_uint(&time_msg);
        if (time_val <= high && time_val >= low){
            if (missed >= offset){
                box_tuple_ref(tmp);
                result[count] = tmp;
                count++;
                if (count >= limit){
                    break;
                }
            } else {
                missed++;
            }
        }
    }
    box_iterator_free(it);
    
    box_txn_commit();

    int i = 0;
    qsort(result, count, sizeof(*result), cmp);

    for (i = 0; i < count; i++){
        box_return_tuple(ctx, result[i]);
        box_tuple_unref(result[i]);
    }

    free(result);
    return 0;
}

int cmp(const void* e1, const void* e2){
    box_tuple_t* t1 = *((box_tuple_t**)e1);
    box_tuple_t* t2 = *((box_tuple_t**)e2);
    
    
    const char* time_msg1 = box_tuple_field(t1, time_field_num);
    uint64_t time_val1 = mp_decode_uint(&time_msg1);

    const char* time_msg2 = box_tuple_field(t2, time_field_num);
    uint64_t time_val2 = mp_decode_uint(&time_msg2);


    return (time_val1 < time_val2) ? 1 : (time_val1 > time_val2) ? -1 : 0;
}
