#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <libgearman/gearman.h>

#include "snowflake.h"

#define ID_MAX_LEN 21
#define EOS(s) ((s)+strlen(s))

typedef struct {
    char *host;
    int port;
} worker_config_st;

static void *gid(gearman_job_st *job, void *context, size_t *result_size, gearman_return_t *ret_ptr) 
{
    char *data;
    int nums;
    const char *workload;
    uint64_t gid;
    char *formatString;
    workload = (const char *)gearman_job_workload(job);

    nums = atoi(workload);
    if (nums < 1) {
        nums = 1;
    }

    fprintf(stdout, "Request numbers: %d\n", nums);

    int dataLength = ID_MAX_LEN * nums + 1;

    data = malloc(dataLength);
    if (data == NULL)
    {
        fprintf(stderr, "malloc result:%d\n", errno);
        *ret_ptr= GEARMAN_WORK_FAIL;
        return NULL;
    }

    int appendLength = 0;
    for (int i = 0; i < nums; i++) {
        gid = snowflake_id((snowflake_st *) context);
        fprintf(stdout, "GID: %"PRIu64", TIMESTAMP: %"PRIu64", NUMS: %d\n", gid, ((snowflake_st *)context)->last_timestamp, i);
        if (i == (nums-1)) {
            formatString = "%"PRIu64;
        } else {
            formatString = "%"PRIu64",";
        }
        appendLength += snprintf(EOS(data), dataLength - appendLength, formatString, gid);
    }

    *ret_ptr = GEARMAN_SUCCESS;
    *result_size = strlen(data);

    return data;
}

int main(int argc, char *argv[])
{
    int opt;
    worker_config_st config = {
        .host = "127.0.0.1",
        .port = 4730,
    };

    snowflake_st snowflake_st = {
        .last_timestamp = 0,
        .datacenter = 1,
        .machine = 1,
        .seq = 0
    };

    while ((opt = getopt(argc, argv, "h:p:d:m:")) != -1) {
        switch (opt) {
            case 'h':
                config.host = optarg;
                break;
            case 'p':
                config.port = atoi(optarg);
                break;
            case 'd':
                snowflake_st.datacenter = atoi(optarg);
                break;
            case 'm':
                snowflake_st.machine = atoi(optarg);
                break;
            default:
                fprintf(stderr, "Usage: %s -h host [-p port] [-d datacenter] [-m machine] \n", argv[0]);
                return EXIT_FAILURE;
        }
    }

    fprintf(stdout, "Job Server: %s, Port: %d, Datacenter: %d, Machine: %d\n", config.host, config.port, snowflake_st.datacenter, snowflake_st.machine);

    if (snowflake_st.datacenter > MAX_DATACENTER_NUM) {
        fprintf(stderr, "Datacenter value too big, the max value is %d\n", MAX_DATACENTER_NUM);
        return EXIT_FAILURE;
    }

    if (snowflake_st.machine > MAX_MACHINE_NUM) {
        fprintf(stderr, "Machine value too big, the max value is %d\n", MAX_MACHINE_NUM);
        return EXIT_FAILURE;
    }

    gearman_worker_st worker;
    gearman_worker_create(&worker);
    gearman_return_t ret = gearman_worker_add_server(&worker, config.host, config.port);
    ret = gearman_worker_add_function(&worker, "gid", 5, gid, &snowflake_st);

    while(1) {
        ret = gearman_worker_work(&worker);
        if (ret != GEARMAN_SUCCESS)
        {
            fprintf(stderr, "%s\n", gearman_worker_error(&worker));
            return EXIT_FAILURE;
        }
    }

    gearman_worker_free(&worker);
    return 0;
}
