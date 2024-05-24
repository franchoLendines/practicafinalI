#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <dirent.h>
#include <time.h>

#define MAX_LINE_LENGTH 256
#define SHM_NAME "/my_shared_memory"
#define MAX_RECORDS 1000
#define MAX_FIELD_LEN 256
#define NUM_SUCURSALES 5
#define NUM_CSV_PER_SUCURSAL 10
#define MAX_PATH_LENGTH 2048  // Aumentado tamaño máximo para las rutas

// Estructura para almacenar registros en la memoria compartida
typedef struct {
    char line[MAX_LINE_LENGTH];
} Record;

// Tipo de datos para manejar los archivos por hilo
typedef struct {
    char** files;  // Array de nombres de archivos
    int count;     // Número de archivos
} ThreadData;

// Variables globales para acceso a la memoria compartida
int shm_fd;
Record *records;
size_t size_fp; // Tamaño de la memoria compartida
size_t used_size = 0;

char directory[MAX_PATH_LENGTH] = "carpetacsv"; // Directorio base
int NUM_PROCESOS;
pthread_mutex_t mutex;
static int headerWritten = 0;  // Indicador de si la cabecera ya fue escrita
static int recordCount = 0;    // Contador de registros

// Función para leer el archivo de configuración
void leerArchivo() {
    FILE *file, *logFile;
    char line[MAX_LINE_LENGTH];
    char *key, *value;
    char archivo[MAX_LINE_LENGTH] = "";
    char ficherolog[MAX_LINE_LENGTH] = "ficherolog.txt"; // Asignación directa si el archivo siempre es el mismo
    char SIMULATE_SLEEP[MAX_LINE_LENGTH] = "";

    logFile = fopen(ficherolog, "a"); // Abre el archivo de log para añadir mensajes

    // Abre el fichero de properties
    file = fopen("config.txt", "r");
    if (!file) {
        perror("No se pudo abrir el archivo");
        fprintf(logFile, "Error: No se pudo abrir el archivo 'config.txt'\n");
        fclose(logFile);
        return;
    }

    // Lee el fichero línea por línea
    while (fgets(line, MAX_LINE_LENGTH, file)) {
        // Elimina el salto de línea al final, si existe
        line[strcspn(line, "\n")] = 0;

        // Divide la línea en clave y valor
        key = strtok(line, "=");
        value = strtok(NULL, "=");

        // Comprueba si la clave y el valor son válidos y guarda los datos
        if (key && value) {
            if (strcmp(key, "directorio") == 0) {
                strncpy(directory, value, MAX_PATH_LENGTH);  // Usar el nombre correcto 'directory'
            } else if (strcmp(key, "archivo") == 0) {
                strncpy(archivo, value, MAX_LINE_LENGTH);
            } else if(strcmp(key, "ficherolog") == 0) {
                strncpy(ficherolog, value, MAX_LINE_LENGTH);
            } else if(strcmp(key, "NUM_PROCESOS") == 0) {
                NUM_PROCESOS = atoi(value);
            } else if(strcmp(key, "SIMULATE_SLEEP") == 0) {
                strncpy(SIMULATE_SLEEP, value, MAX_LINE_LENGTH);
            } else if(strcmp(key, "size_fp") == 0) {
                size_fp = atol(value);  // Leer tamaño de la memoria compartida
            }
        }
    }

    fclose(file);
    fclose(logFile); // Cierra el archivo de log
}

// Función para expandir la memoria compartida cuando se necesita más espacio
void expandirMemoriaCompartida(size_t new_size) {
    // Desmapear la memoria actual
    if (munmap(records, size_fp) == -1) {
        perror("munmap");
        exit(EXIT_FAILURE);
    }

    // Redimensionar el archivo de memoria compartida
    if (ftruncate(shm_fd, new_size) == -1) {
        perror("ftruncate");
        exit(EXIT_FAILURE);
    }

    // Mapear la memoria con el nuevo tamaño
    records = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (records == MAP_FAILED) {
        perror("mmap");
        exit(EXIT_FAILURE);
    }

    size_fp = new_size;
    printf("Memoria compartida expandida a %zu bytes\n", size_fp);
}

// Función que ejecuta cada hilo para leer archivos CSV y almacenar en la memoria compartida
void* leerficheros(void* arg) {
    ThreadData *data = (ThreadData *)arg;

    FILE *logFile = fopen("ficherolog.txt", "a"); // Abre el archivo de log para añadir mensajes

    for (int i = 0; i < data->count; i++) {
        char filePath[MAX_PATH_LENGTH];
        snprintf(filePath, sizeof(filePath), "%s", data->files[i]);
        printf("Leyendo archivo: %s\n", filePath); // Mensaje de depuración
        FILE* csvFile = fopen(filePath, "r");
        if (csvFile) {
            char line[MAX_LINE_LENGTH];
            int isFirstLine = 1; // Variable para controlar la primera línea (cabecera) de cada archivo
            while (fgets(line, sizeof(line), csvFile)) {
                pthread_mutex_lock(&mutex);
                // Comprobar si hay suficiente espacio en la memoria compartida
                if (used_size + sizeof(Record) > size_fp) {
                    expandirMemoriaCompartida(size_fp * 2);  // Duplicar el tamaño de la memoria compartida
                }
                if (isFirstLine) {
                    if (!headerWritten) {
                        strncpy(records[recordCount].line, line, MAX_LINE_LENGTH);
                        recordCount++;
                        used_size += sizeof(Record);
                        fprintf(logFile, "Cabecera: %s", line); // Escribe la cabecera en el log
                        headerWritten = 1; // Se marca que la cabecera ya fue escrita
                        printf("Cabecera escrita: %s\n", line); // Mensaje de depuración
                    }
                    isFirstLine = 0; // Se marca para no volver a entrar en este bloque para el archivo actual
                    pthread_mutex_unlock(&mutex); // Desbloquear el mutex aquí
                    continue; // Salta el resto del ciclo para la primera línea después de escribirla (si es necesario)
                }
                strncpy(records[recordCount].line, line, MAX_LINE_LENGTH);
                recordCount++;
                used_size += sizeof(Record);
                fprintf(logFile, "%s", line); // Escribe cada línea en el log
                printf("Linea escrita: %s\n", line); // Mensaje de depuración
                pthread_mutex_unlock(&mutex); // Desbloquear el mutex aquí
            }
            fclose(csvFile);
        } else {
            perror("Error abriendo archivo CSV");
            fprintf(logFile, "Error: No se pudo abrir el archivo CSV '%s'\n", filePath);
        }
    }

    fclose(logFile); // Cierra el archivo de log
    return NULL;
}

// Función para crear archivos CSV con datos aleatorios
void crearcsv() {
    char respuesta;
    printf("¿Desea crear los archivos CSV? (s/n): ");
    scanf(" %c", &respuesta);

    if (respuesta == 's' || respuesta == 'S') {
        srand(time(NULL)); // Inicializa la semilla del generador de números aleatorios
        const char *usuarios[] = {
            "USER001", "USER002", "USER003", "USER004", "USER005",
            "USER006", "USER007", "USER008", "USER009", "USER010"
        };
        int i;
        for (i = 1; i <= 50; i++) {
            char filename[MAX_PATH_LENGTH]; // Aumenta el tamaño para acomodar la ruta de la carpeta
            // Calcula el número de sucursal basado en el índice del archivo
            int sucursal_numero = (i - 1) / 10 + 1; 
            // Formatea el nombre del archivo con la carpeta correspondiente
            snprintf(filename, sizeof(filename), "carpetacsv/Sucursal%d/archivo_%02d.csv", sucursal_numero, i);
            FILE *file = fopen(filename, "w+");
            if (file == NULL) {
                printf("Error al crear el archivo %s\n", filename);
                exit(1);
            }
            // Escribe contenido en el archivo CSV
            fprintf(file, "IdOperacion;FECHA_INICIO;FECHA_FIN;IdUsuario;IdTipoOperacion;Importe;Saldo;Estado\n");
            int j;
            for (j = 1; j <= 10; j++) { // Genera 10 líneas de datos aleatorios
                time_t t = time(NULL);
                struct tm *tm = localtime(&t);
                tm->tm_year = 124; // Ajusta el año a 2024
                tm->tm_mon = rand() % 12; // Mes aleatorio
                tm->tm_mday = rand() % 28 + 1; // Día aleatorio
                tm->tm_hour = rand() % 24; // Hora aleatoria
                tm->tm_min = rand() % 60; // Minutos aleatorios
                char fecha_inicio[20], fecha_fin[20];
                strftime(fecha_inicio, sizeof(fecha_inicio), "%d/%m/%Y %H:%M", tm);

                int minutos_extra = rand() % 60 + 1; // Genera un número aleatorio de minutos para agregar
                tm->tm_min += minutos_extra; // Ajusta los minutos para la fecha de fin
                mktime(tm); // Normaliza la fecha en caso de que los minutos hayan pasado de 60
                strftime(fecha_fin, sizeof(fecha_fin), "%d/%m/%Y %H:%M", tm);

                const char *usuario = usuarios[rand() % 10]; // Selecciona un usuario aleatorio
                fprintf(file, "OPE%03d;%s;%s;%s;COMPRA%02d;%d;%d.0€;%s\n",
                        j, fecha_inicio, fecha_fin, usuario, rand() % 4, rand() % 5, rand() % 1001 - 500, rand() % 2 ? "Correcto" : "Error");
            }
            fclose(file);
        }
        printf("Se han creado 50 archivos CSV con datos aleatorios distribuidos en 5 sucursales.\n");
    } else {
        printf("Operación cancelada por el usuario.\n");
    }
}

// Función para extraer los datos de la memoria compartida a un fichero consolidado
void extraer_a_fichero() {
    char respuesta;
    printf("¿Desea extraer los datos a un fichero consolidado? (s/n): ");
    scanf(" %c", &respuesta);

    if (respuesta == 's' || respuesta == 'S') {
        FILE *file = fopen("consolidado.csv", "w");
        if (file == NULL) {
            perror("Error al crear el archivo consolidado.csv");
            return;
        }

        // Escribir las líneas desde la memoria compartida al archivo
        for (int i = 0; i < recordCount; i++) {
            fprintf(file, "%s", records[i].line);
        }

        fclose(file);
        printf("Datos extraídos a consolidado.csv correctamente.\n");
    } else {
        printf("Operación cancelada por el usuario.\n");
    }
}

// Función para finalizar la ejecución del programa
void finalizar_programa() {
    char respuesta;
    printf("¿Desea terminar la ejecución del programa? (s/n): ");
    scanf(" %c", &respuesta);

    if (respuesta == 's' || respuesta == 'S') {
        printf("Finalizando la ejecución del programa.\n");
        exit(0);  // Terminar la ejecución del programa
    }
}

int main() {
    // Redirigir stdout y stderr a ficherolog.txt
    freopen("ficherolog.txt", "a", stdout);
    freopen("ficherolog.txt", "a", stderr);

    pthread_mutex_init(&mutex, NULL); // Inicializar el mutex
    leerArchivo();  // Carga la configuración del archivo
    crearcsv();     // Pregunta y potencialmente ejecuta la creación de archivos CSV

    // Crear y mapear la memoria compartida
    shm_fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
    if (shm_fd == -1) {
        perror("shm_open");
        exit(EXIT_FAILURE);
    }

    if (ftruncate(shm_fd, size_fp) == -1) {
        perror("ftruncate");
        exit(EXIT_FAILURE);
    }

    records = mmap(NULL, size_fp, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (records == MAP_FAILED) {
        perror("mmap");
        exit(EXIT_FAILURE);
    }

    // Verificar existencia del directorio base
    printf("Verificando directorio base: %s\n", directory);
    DIR *baseDir = opendir(directory);
    if (baseDir == NULL) {
        perror("Error al abrir el directorio base");
        exit(EXIT_FAILURE);
    }
    closedir(baseDir);

    // Variables para almacenar archivos
    char *files[NUM_SUCURSALES * NUM_CSV_PER_SUCURSAL];
    int totalFiles = 0;

    // Recorrer las carpetas de Sucursal1 a Sucursal5
    for (int i = 1; i <= NUM_SUCURSALES; i++) {
        char subDir[MAX_PATH_LENGTH];
        snprintf(subDir, sizeof(subDir), "%s/Sucursal%d", directory, i);

        printf("Abriendo subdirectorio: %s\n", subDir); // Mensaje de depuración

        DIR *dir = opendir(subDir);
        if (dir == NULL) {
            perror("Error al abrir el subdirectorio");
            printf("Error al abrir el subdirectorio: %s\n", subDir); // Mensaje de depuración
            continue; // Continuar con el siguiente subdirectorio
        }

        struct dirent *entry;
        while ((entry = readdir(dir)) != NULL) {
            if (entry->d_type == DT_REG && strstr(entry->d_name, ".csv")) {
                char *filePath = malloc(MAX_PATH_LENGTH);
                snprintf(filePath, MAX_PATH_LENGTH, "%s/%s", subDir, entry->d_name);
                files[totalFiles++] = filePath;
            }
        }
        closedir(dir);
    }

    // Verificar si se encontraron archivos CSV
    if (totalFiles == 0) {
        printf("No se encontraron archivos CSV en las subcarpetas.\n");
        exit(EXIT_FAILURE);
    }

    printf("Total archivos CSV encontrados: %d\n", totalFiles); // Mensaje de depuración

    // Dividir archivos entre los procesos (hilos)
    int filesPerThread = totalFiles / NUM_PROCESOS;
    int remainingFiles = totalFiles % NUM_PROCESOS;
    pthread_t threads[NUM_PROCESOS];
    ThreadData data[NUM_PROCESOS];

    for (int i = 0, j = 0; i < NUM_PROCESOS; i++) {
        data[i].files = &files[j];
        data[i].count = (i < remainingFiles) ? filesPerThread + 1 : filesPerThread;
        j += data[i].count;
        if (pthread_create(&threads[i], NULL, leerficheros, (void *)&data[i]) != 0) {
            perror("pthread_create");
            exit(EXIT_FAILURE);
        }
    }

    // Esperar a que todos los hilos terminen
    for (int i = 0; i < NUM_PROCESOS; i++) {
        if (pthread_join(threads[i], NULL) != 0) {
            perror("pthread_join");
            exit(EXIT_FAILURE);
        }
    }

    // Mostrar contenido de la memoria compartida
    for (int i = 0; i < recordCount; i++) {
        printf("%s", records[i].line);
    }

    // Preguntar si se desea terminar el programa
    finalizar_programa();

    // Preguntar si se desea extraer los datos a un fichero consolidado
    extraer_a_fichero();

    // Liberar recursos y cerrar
    pthread_mutex_destroy(&mutex);
    for (int i = 0; i < totalFiles; i++) {
        free(files[i]);  // Libera la memoria de los nombres de los archivos
    }

    // Desmapear y cerrar la memoria compartida
    munmap(records, size_fp);
    close(shm_fd);

    return 0;
}
