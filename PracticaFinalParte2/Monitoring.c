#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#define MAX_TRANSACCIONES 10000
#define LIMITE_TRANSACCIONES 5
#define LIMITE_RETIROS 3
#define SHM_NAME "/my_shared_memory"
#define MAX_LINE_LENGTH 256

// Estructura para almacenar información de una transacción
typedef struct {
    char IdOperacion[10];
    char IdUsuario[50];
    char FECHA_INICIO[20];
    char FECHA_FIN[20];
    char IdTipoOperacion[25];
    float Importe;
    float Saldo;
    char Estado[25];
} Transaccion;

Transaccion transacciones[MAX_TRANSACCIONES]; // Array para almacenar las transacciones
int num_transacciones = 0; // Contador de transacciones
pthread_mutex_t mutex; // Mutex para controlar el acceso a transacciones[]

typedef struct {
    char line[MAX_LINE_LENGTH];
} Record;

Record *records; // Puntero a los registros en la memoria compartida
int shm_fd; // Descriptor de archivo para la memoria compartida

// Función para leer las transacciones desde la memoria compartida
void leer_transacciones_memoria_compartida() {
    // Abrir la memoria compartida
    shm_fd = shm_open(SHM_NAME, O_RDONLY, 0666);
    if (shm_fd == -1) {
        perror("shm_open");
        exit(EXIT_FAILURE);
    }

    // Obtener el tamaño de la memoria compartida
    struct stat shm_stat;
    if (fstat(shm_fd, &shm_stat) == -1) {
        perror("fstat");
        exit(EXIT_FAILURE);
    }

    // Mapear la memoria compartida
    records = mmap(NULL, shm_stat.st_size, PROT_READ, MAP_SHARED, shm_fd, 0);
    if (records == MAP_FAILED) {
        perror("mmap");
        exit(EXIT_FAILURE);
    }

    // Leer las transacciones desde la memoria compartida
    char buffer[MAX_LINE_LENGTH];
    for (int i = 0; i < MAX_TRANSACCIONES && records[i].line[0] != '\0'; i++) {
        strncpy(buffer, records[i].line, MAX_LINE_LENGTH);
        // Parsear la línea en el formato esperado y almacenar en la estructura de transacciones
        if (sscanf(buffer, "%[^;];%[^;];%[^;];%[^;];%[^;];%f;%f€;%[^;\n]",
               transacciones[num_transacciones].IdOperacion,
               transacciones[num_transacciones].FECHA_INICIO,
               transacciones[num_transacciones].FECHA_FIN,
               transacciones[num_transacciones].IdUsuario,
               transacciones[num_transacciones].IdTipoOperacion,
               &transacciones[num_transacciones].Importe,
               &transacciones[num_transacciones].Saldo,
               transacciones[num_transacciones].Estado) == 8) {
            printf("Importe leído: %.2f\n", transacciones[num_transacciones].Importe); // Debug
            num_transacciones++;
            printf("Transacción %d cargada correctamente.\n", num_transacciones);
        } else {
            printf("Error al parsear la línea: %s\n", buffer);
        }
    }
    printf("Total de transacciones cargadas: %d\n", num_transacciones);
}

// Función para extraer la fecha en formato "dia/mes/año"
void extraer_fecha_dmy(const char* fechaHora, char* fecha) {
    sscanf(fechaHora, "%[^ ]", fecha); // Extrae hasta el primer espacio (fecha sin hora)
}

// Función para extraer la fecha y hora hasta las horas en formato "dd/mm/yyyy hh"
void extraer_fecha_hora(const char* fechaHora, char* fecha) {
    strncpy(fecha, fechaHora, 13);
    fecha[13] = '\0';
}

// Función auxiliar para verificar si una operación es de retiro (saldo negativo)
int esRetiro(float Saldo) {
    return Saldo < 0;
}

// Función para detectar patrón 1
void* patron1(void* arg) {
    pthread_mutex_lock(&mutex);
    printf("Procesando %d transacciones en el patrón 1.\n", num_transacciones);
    for (int i = 0; i < num_transacciones; i++) {
        int contador = 1; // Contador inicia en 1 para contar la transacción actual
        char fecha_hora_i[20];
        extraer_fecha_hora(transacciones[i].FECHA_INICIO, fecha_hora_i);
        printf("Usuario: %s, FechaHora: %s\n", transacciones[i].IdUsuario, fecha_hora_i); // Debug
        for (int j = 0; j < num_transacciones; j++) {
            if (i != j) {
                char fecha_hora_j[20];
                extraer_fecha_hora(transacciones[j].FECHA_INICIO, fecha_hora_j);
                if (strcmp(transacciones[i].IdUsuario, transacciones[j].IdUsuario) == 0 &&
                    strcmp(fecha_hora_i, fecha_hora_j) == 0) {
                    contador++;
                }
            }
        }
        printf("Usuario: %s, FechaHora: %s, Transacciones: %d\n", transacciones[i].IdUsuario, fecha_hora_i, contador); // Debug
        if (contador > LIMITE_TRANSACCIONES) {
            printf("%s ha dado positivo en el patron 1.\n", transacciones[i].IdUsuario);
        }
    }
    pthread_mutex_unlock(&mutex);
    return NULL;
}

// Función para detectar patrón 2
void* patron2(void* arg) {
    pthread_mutex_lock(&mutex);
    printf("Procesando %d transacciones en el patrón 2.\n", num_transacciones);
    for (int i = 0; i < num_transacciones; i++) {
        char fecha_i[20];
        extraer_fecha_dmy(transacciones[i].FECHA_INICIO, fecha_i);
        int contador_retiros = esRetiro(transacciones[i].Saldo) ? 1 : 0; // Inicializa el contador de retiros
        
        for (int j = 0; j < num_transacciones; j++) {
            if (i != j && strcmp(transacciones[i].IdUsuario, transacciones[j].IdUsuario) == 0) {
                char fecha_j[20];
                extraer_fecha_dmy(transacciones[j].FECHA_INICIO, fecha_j);
                if (strcmp(fecha_i, fecha_j) == 0 && esRetiro(transacciones[j].Saldo)) {
                    contador_retiros++;
                }
            }
        }

        printf("Usuario: %s, Fecha: %s, Retiros: %d\n", transacciones[i].IdUsuario, fecha_i, contador_retiros); // Debug
        if (contador_retiros >= 3) {
            printf("%s ha dado positivo en el patron 2.\n", transacciones[i].IdUsuario);
        }
    }
    pthread_mutex_unlock(&mutex);
    return NULL;
}

// Función para detectar patrón 3
void* patron3(void* arg) {
    pthread_mutex_lock(&mutex);
    printf("Procesando %d transacciones en el patrón 3.\n", num_transacciones);
    for (int i = 0; i < num_transacciones; i++) {
        char fecha_i[20];
        extraer_fecha_dmy(transacciones[i].FECHA_INICIO, fecha_i);
        int contador_errores = (strcmp(transacciones[i].Estado, "Error") == 0) ? 1 : 0; // Inicializa el contador de errores

        for (int j = i + 1; j < num_transacciones; j++) {
            if (strcmp(transacciones[i].IdUsuario, transacciones[j].IdUsuario) == 0) {
                char fecha_j[20];
                extraer_fecha_dmy(transacciones[j].FECHA_INICIO, fecha_j);
                if (strcmp(fecha_i, fecha_j) == 0 && strcmp(transacciones[j].Estado, "Error") == 0) {
                    contador_errores++;
                }
            }
        }

        for (int k = i + 1; k < num_transacciones; k++) {
            if (strcmp(transacciones[i].IdUsuario, transacciones[k].IdUsuario) == 0) {
                char fecha_k[20];
                extraer_fecha_dmy(transacciones[k].FECHA_INICIO, fecha_k);
                if (strcmp(fecha_i, fecha_k) == 0 && strcmp(transacciones[k].Estado, "Error") == 0) {
                    contador_errores++;
                }
            }
        }

        printf("Usuario: %s, Fecha: %s, Errores: %d\n", transacciones[i].IdUsuario, fecha_i, contador_errores); // Debug
        if (contador_errores >= 3) {
            printf("%s ha dado positivo en el patron 3.\n", transacciones[i].IdUsuario);
        }
    }
    pthread_mutex_unlock(&mutex);
    return NULL;
}

// Función para detectar patrón 4
void* patron4(void* arg) {
    pthread_mutex_lock(&mutex);
    printf("Procesando %d transacciones en el patrón 4.\n", num_transacciones);
    for (int i = 0; i < num_transacciones; i++) {
        int compra00 = 0, compra01 = 0, compra02 = 0, compra03 = 0;
        char fecha_i[20];
        extraer_fecha_dmy(transacciones[i].FECHA_INICIO, fecha_i);
        printf("Usuario: %s, Fecha: %s, Tipo de operacion: %s\n", transacciones[i].IdUsuario, fecha_i, transacciones[i].IdTipoOperacion); // Debug
        for (int j = 0; j < num_transacciones; j++) {
            if (i != j) {
                char fecha_j[20];
                extraer_fecha_dmy(transacciones[j].FECHA_INICIO, fecha_j);
                if (strcmp(transacciones[i].IdUsuario, transacciones[j].IdUsuario) == 0 &&
                    strcmp(fecha_i, fecha_j) == 0) {
                    if (strcmp(transacciones[j].IdTipoOperacion, "COMPRA00") == 0) compra00 = 1;
                    if (strcmp(transacciones[j].IdTipoOperacion, "COMPRA01") == 0) compra01 = 1;
                    if (strcmp(transacciones[j].IdTipoOperacion, "COMPRA02") == 0) compra02 = 1;
                    if (strcmp(transacciones[j].IdTipoOperacion, "COMPRA03") == 0) compra03 = 1;
                }
            }
        }
        if (compra00 && compra01 && compra02 && compra03) {
            printf("%s ha dado positivo en el patron 4.\n", transacciones[i].IdUsuario);
        }
    }
    pthread_mutex_unlock(&mutex);
    return NULL;
}

// Función para detectar patrón 5
void* patron5(void* arg) {
    pthread_mutex_lock(&mutex);
    printf("Procesando %d transacciones en el patrón 5.\n", num_transacciones);
    for (int i = 0; i < num_transacciones; i++) {
        float total_retirado = 0, total_ingresado = 0;
        char fecha_i[20];
        extraer_fecha_dmy(transacciones[i].FECHA_INICIO, fecha_i);
        printf("Usuario: %s, Fecha: %s\n", transacciones[i].IdUsuario, fecha_i); // Debug
        for (int j = 0; j < num_transacciones; j++) {
            if (i != j) {
                char fecha_j[20];
                extraer_fecha_dmy(transacciones[j].FECHA_INICIO, fecha_j);
                if (strcmp(transacciones[i].IdUsuario, transacciones[j].IdUsuario) == 0 &&
                    strcmp(fecha_i, fecha_j) == 0) {
                    if (esRetiro(transacciones[j].Importe)) {
                        total_retirado += transacciones[j].Importe;  // Sumar Importe negativo
                    } else {
                        total_ingresado += transacciones[j].Importe;  // Sumar Importe positivo
                    }
                }
            }
        }
        printf("Usuario: %s, Total Retirado: %.2f, Total Ingresado: %.2f\n", transacciones[i].IdUsuario, total_retirado, total_ingresado); // Debug
        if (total_retirado > total_ingresado) {
            printf("%s ha dado positivo en el patron 5 por tener más retiros que ingresos en un día.\n", transacciones[i].IdUsuario);
        }
    }
    pthread_mutex_unlock(&mutex);
    return NULL;
}

int main() {
    // Redirigir stdout y stderr a ficherolog.txt
    freopen("ficherolog.txt", "a", stdout);
    freopen("ficherolog.txt", "a", stderr);

    pthread_t thread1, thread2, thread3, thread4, thread5;
    pthread_mutex_init(&mutex, NULL); // Inicializar el mutex

    leer_transacciones_memoria_compartida(); // Leer transacciones desde la memoria compartida

    // Crear hilos para los patrones
    if (pthread_create(&thread1, NULL, patron1, NULL) != 0 ||
        pthread_create(&thread2, NULL, patron2, NULL) != 0 ||
        pthread_create(&thread3, NULL, patron3, NULL) != 0 ||
        pthread_create(&thread4, NULL, patron4, NULL) != 0 ||
        pthread_create(&thread5, NULL, patron5, NULL) != 0) {
        perror("pthread_create");
        exit(EXIT_FAILURE);
    }

    // Esperar a que los hilos terminen
    if (pthread_join(thread1, NULL) != 0 ||
        pthread_join(thread2, NULL) != 0 ||
        pthread_join(thread3, NULL) != 0 ||
        pthread_join(thread4, NULL) != 0 ||
        pthread_join(thread5, NULL) != 0) {
        perror("pthread_join");
        exit(EXIT_FAILURE);
    }

    pthread_mutex_destroy(&mutex); // Destruir el mutex al finalizar

    // Desmapear y cerrar la memoria compartida
    munmap(records, MAX_TRANSACCIONES * sizeof(Record));
    close(shm_fd);

    return 0;
}
