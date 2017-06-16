#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "snowflake.h"

static uint64_t snowflake_next_timestamp();

uint64_t last_timestamp = 0;
int seq = 0;

uint64_t snowflake_id(int datacenter, int machine)
{/*{{{*/
    if (datacenter > MAX_DATACENTER_NUM) {
        fprintf(stderr, "Datacenter value too big, the max value is %d\n", MAX_DATACENTER_NUM);
        return 0;
    }

    if (machine > MAX_MACHINE_NUM) {
        fprintf(stderr, "Machine value too big, the max value is %d\n", MAX_MACHINE_NUM);
        return 0;
    }

    uint64_t current_timestamp = snowflake_timestamp();

    if (current_timestamp == last_timestamp) {
        seq = (seq + 1) & MAX_SEQ_NUM;

        if (seq == 0) {
            current_timestamp = snowflake_next_timestamp();
        }
    } else {
        seq = 0;
    }

    last_timestamp = current_timestamp;

    return (current_timestamp - START_TIMESTAMP) << TIMESTAMP_LEFT_OFFSET | datacenter << DATACENTER_LEFT_OFFSET | machine << MACHINE_LEFT_OFFSET | seq;
}/*}}}*/

uint64_t snowflake_timestamp() 
{/*{{{*/
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + (uint64_t)tv.tv_usec / 1000;
}/*}}}*/

static uint64_t snowflake_next_timestamp()
{/*{{{*/
    uint64_t cur;
    do {
        cur = snowflake_timestamp();
    } while (cur <= last_timestamp);
    return cur;
}/*}}}*/

/* int main(int argc, const char *argv[]) */
/* { */
    /* printf("%lld\n", getID()); */
    /* return 0; */
/* } */
