#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <libgearman/gearman.h>

#include "snowflake.h"

struct Config {
    int datacenter;
    int machine;
    char *host;
    int port;
};

static void *gid(gearman_job_st *job, void *context, size_t *result_size, gearman_return_t *ret_ptr) 
{
    char *data;
    data = malloc(sizeof(uint64_t));
    if (data == NULL)
    {
        printf("malloc result:%d\n", errno);
        *ret_ptr= GEARMAN_WORK_FAIL;
        return NULL;
    }

    sprintf(data, "%lld", snowflake_id(((struct Config *) context)->datacenter, ((struct Config *)context)->machine));

    *ret_ptr = GEARMAN_SUCCESS;
    *result_size = strlen(data);

    return data;
}

int main(int argc, char *argv[])
{
    int opt;
    struct Config config = {
        .datacenter = 1,
        .machine = 1,
        .host = "127.0.0.1",
        .port = 4730
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
                config.datacenter = atoi(optarg);
                break;
            case 'm':
                config.machine = atoi(optarg);
                break;
            default:
                fprintf(stderr, "Usage: %s -h host [-p port] [-d datacenter] [-m machine] \n", argv[0]);
                return EXIT_FAILURE;
        }
    }

    fprintf(stdout, "Job Server: %s, Port: %d, Datacenter: %d, Machine: %d\n", config.host, config.port, config.datacenter, config.machine);

    if (config.datacenter > MAX_DATACENTER_NUM) {
        fprintf(stderr, "Datacenter value too big, the max value is %d\n", MAX_DATACENTER_NUM);
        return EXIT_FAILURE;
    }

    if (config.machine > MAX_MACHINE_NUM) {
        fprintf(stderr, "Machine value too big, the max value is %d\n", MAX_MACHINE_NUM);
        return EXIT_FAILURE;
    }

    gearman_worker_st worker;
    gearman_worker_create(&worker);
    gearman_return_t ret = gearman_worker_add_server(&worker, config.host, config.port);
    ret = gearman_worker_add_function(&worker, "gid", 5, gid, &config);

    while(1) {
        ret = gearman_worker_work(&worker);
        if (ret != GEARMAN_SUCCESS)
        {
            printf("%s\n", gearman_worker_error(&worker));
            return EXIT_FAILURE;
        }
    }

    gearman_worker_free(&worker);
    return 0;
}
