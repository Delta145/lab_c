#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <semaphore.h>
#include <sys/mman.h>

#define log_level 1
#define megabyte_size 1024*1024
#define A 50
#define B 0xDEADBEEF
#define D 30
#define E 11
#define G 24
#define I 20

typedef struct {
    int thread_number;
    int ints_per_thread;
    int *start;
    FILE *file;
} thread_generator_data;

typedef struct {
    int ints_per_file;
    int files;
    int *start;
    int *end;
    sem_t *semaphores;
} thread_writer_data;

typedef struct {
    int thread_number;
    int file_number;
    sem_t *semaphores;
} thread_reader_data;

int read_int_from_file(FILE *file) {
    int i = 0;
    fread(&i, 4, 1, file);
    return i;
}

void *fill_with_random(void *thread_data) {
    thread_generator_data *data = (thread_generator_data *) thread_data;
    if (log_level > 1) {
        printf("[GENERATOR-%d] started...\n", data->thread_number);
    }
    while (1) {
        for (int i = 0; i < data->ints_per_thread; i++) {
            data->start[i] = read_int_from_file(data->file);
        }
    }
    return NULL;
}

void *read_files(void *thread_data) {
    thread_reader_data *data = (thread_reader_data *) thread_data;
    if (log_level > 1) {
        printf("[READER-%d] started...\n", data->thread_number);
    }
    while (1) {
        char filename[6] = "lab1_0";
        filename[5] = '0' + data->file_number;
        FILE *file_ptr = NULL;
        while (file_ptr == NULL) {
            sem_wait(&data->semaphores[data->file_number]);
            file_ptr = fopen(filename, "rb");
            if (file_ptr == NULL) {
                sem_post(&data->semaphores[data->file_number]);
                if (log_level > 2) {
                    printf("[READER-%d] I/O error on open file %s.\n", data->thread_number, filename);
                }
            }
        }
        fseek(file_ptr, 0, SEEK_END);
        long file_size = ftell(file_ptr);
        rewind(file_ptr);
        int *buffer = (int *) malloc(file_size);
        unsigned long result = fread(buffer, 1, file_size, file_ptr);
        fclose(file_ptr);
        sem_post(&data->semaphores[data->file_number]);

        if (result != file_size) {
            fputs("Ошибка чтения", stderr);
            exit(-1);
        }
        long avg = 0;
        for (int i = 0; i < file_size / 4; i++) {
            avg += buffer[i];
//        if (buffer[i] < avg) {
//            avg = buffer[i];
//        }
        }
        avg = avg / (file_size / 4);

        if (log_level > 0) {
            printf("[READER-%d] file %s avg is %ld.\n", data->thread_number, filename, avg);
        }

        free(buffer);
    }
    return NULL;
}

void write_block(void *ptr, int size, int n, FILE *s) {
    int bytes = size * n;
    int blocks = bytes / G;
    int last_block_size = bytes % G;
    for (int i = 0; i < blocks; ++i) {
        fwrite(ptr, G, 1, s);
        ptr += G;
    }
    if (last_block_size > 0) {
        fwrite(ptr, last_block_size, 1, s);
    }
}

void *write_to_files(void *thread_data) {
    thread_writer_data *data = (thread_writer_data *) thread_data;
    int *write_pointer = data->start;
    while (1) {
        for (int i = 0; i < data->files; i++) {
            char filename[6] = "lab1_0";
            filename[5] = '0' + i;
            sem_wait(&data->semaphores[i]);
            FILE *current_file = fopen(filename, "wb");
            int ints_to_file = data->ints_per_file;
            int is_done = 0;
            while (!is_done) {
                if (ints_to_file + write_pointer < data->end) {
//                    fwrite(write_pointer, sizeof(int), ints_to_file, current_file);
                    write_block(write_pointer, sizeof(int), ints_to_file, current_file);
                    write_pointer += ints_to_file;
                    is_done = 1;
                } else {
                    int available = data->end - write_pointer;
//                    fwrite(write_pointer, sizeof(int), available, current_file);
                    write_block(write_pointer, sizeof(int), available, current_file);
                    write_pointer = data->start;
                    ints_to_file -= available;
                }
            }
            fclose(current_file);
            sem_post(&data->semaphores[i]);
        }
    }
    return NULL;
}

int main() {
    const char *devurandom_filename = "/dev/urandom";
    FILE *devurandom_file = fopen(devurandom_filename, "r");

//    int *memory_region = (int *) malloc(A * megabyte_size);
    int *memory_region = mmap(B, A * megabyte_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    int *thread_data_start = memory_region;

    // generator threads start

    pthread_t *generator_threads = (pthread_t *) malloc(D * sizeof(pthread_t));
    thread_generator_data *generator_data = (thread_generator_data *) malloc(D * sizeof(thread_generator_data));

    int ints = A * 1024 * 256;
    int ints_per_thread = ints / D;
    for (int i = 0; i < D; ++i) {
        generator_data[i].thread_number = i;
        generator_data[i].ints_per_thread = ints_per_thread;
        generator_data[i].start = thread_data_start;
        generator_data[i].file = devurandom_file;
        thread_data_start += ints_per_thread;
    }
    generator_data[D - 1].ints_per_thread += ints % D;

    // generator threads end

    // writer thread start

    int files = A / E;
    if (A % E != 0) {
        files++;
    }

    sem_t semaphore[files];
    for (int i = 0; i < files; ++i) {
        sem_init(&semaphore[i], 0, 1);
    }

    thread_writer_data *writer_data = (thread_writer_data *) malloc(sizeof(thread_writer_data));
    pthread_t *thread_writer = (pthread_t *) malloc(sizeof(pthread_t));
    writer_data->ints_per_file = E * 1024 * 256;
    writer_data->files = files;
    writer_data->start = memory_region;
    writer_data->end = memory_region + ints;
    writer_data->semaphores = semaphore;

    // writer thread end

    // reader threads start

    pthread_t *reader_threads = (pthread_t *) malloc(I * sizeof(pthread_t));
    thread_reader_data *reader_data = (thread_reader_data *) malloc(I * sizeof(thread_reader_data));
    int file_number = 0;
    for (int i = 0; i < I; ++i) {
        if (file_number >= files) {
            file_number = 0;
        }
        reader_data[i].thread_number = i;
        reader_data[i].file_number = file_number;
        reader_data[i].semaphores = semaphore;
        file_number++;
    }

    // reader threads end

    for (int i = 0; i < D; ++i) {
        pthread_create(&(generator_threads[i]), NULL, fill_with_random, &generator_data[i]);
    }
//    for (int i = 0; i < I; i++) {
//        pthread_join(generator_threads[i], NULL);
//    }

    pthread_create(thread_writer, NULL, write_to_files, writer_data);
//    pthread_join(*thread_writer, NULL);

    for (int i = 0; i < I; ++i) {
        pthread_create(&(reader_threads[i]), NULL, read_files, &reader_data[i]);
    }
    for (int i = 0; i < I; i++) {
        pthread_join(reader_threads[i], NULL);
    }


    for (int i = 0; i < files; ++i) {
        sem_destroy(&semaphore[i]);
    }

    fclose(devurandom_file);

    free(generator_threads);
    free(generator_data);
//    free(memory_region);
    munmap(memory_region, A * megabyte_size);
    return 0;
}
