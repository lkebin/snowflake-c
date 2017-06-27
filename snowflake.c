#include "snowflake.h"

static uint64_t snowflake_next_timestamp(uint64_t last_timestamp);

/* uint64_t snowflake_id(uint64_t *last_timestamp,int datacenter, int machine, int *seq) */
uint64_t snowflake_id(struct snowflake_st *snowflake_st)
{/*{{{*/
    uint64_t current_timestamp = snowflake_timestamp();

    if (current_timestamp == snowflake_st->last_timestamp) {
        snowflake_st->seq = (snowflake_st->seq + 1) & MAX_SEQ_NUM;

        if (snowflake_st->seq == 0) {
            current_timestamp = snowflake_next_timestamp(snowflake_st->last_timestamp);
        }
    } else {
        snowflake_st->seq = 0;
    }

    snowflake_st->last_timestamp = current_timestamp;

    return (current_timestamp - START_TIMESTAMP) << TIMESTAMP_LEFT_OFFSET | snowflake_st->datacenter << DATACENTER_LEFT_OFFSET | snowflake_st->machine << MACHINE_LEFT_OFFSET | snowflake_st->seq;
}/*}}}*/

uint64_t snowflake_timestamp() 
{/*{{{*/
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + (uint64_t)tv.tv_usec / 1000;
}/*}}}*/

static uint64_t snowflake_next_timestamp(uint64_t last_timestamp)
{/*{{{*/
    uint64_t cur;
    do {
        cur = snowflake_timestamp();
    } while (cur <= last_timestamp);
    return cur;
}/*}}}*/
